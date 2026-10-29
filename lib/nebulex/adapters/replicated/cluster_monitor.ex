defmodule Nebulex.Adapters.Replicated.ClusterMonitor do
  @moduledoc false

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapters.Replicated.{Bootstrapper, Replicator}
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry

  # State
  defstruct [:adapter_meta, :pg_group, :ready_pg_group, :pg_ref, :cluster_nodes, ready?: false]

  ## API

  @doc false
  def start_link(adapter_meta) do
    GenServer.start_link(__MODULE__, adapter_meta, name: server_name(adapter_meta))
  end

  @doc false
  @spec server_name(map()) :: atom()
  def server_name(%{name: name}) do
    camelize_and_concat([name, ClusterMonitor])
  end

  @doc false
  @spec bootstrap_completed(map()) :: :ok
  def bootstrap_completed(adapter_meta) do
    GenServer.cast(server_name(adapter_meta), :bootstrap_completed)
  end

  ## GenServer Callbacks

  @impl true
  def init(%{pg_group: pg_group, ready_pg_group: ready_pg_group} = adapter_meta) do
    # Trap exit signals to run cleanup on termination
    _ = Process.flag(:trap_exit, true)

    # Subscribe to PG scope events for cluster membership changes
    pg_ref = Cluster.monitor_scope()

    # Join the PG group
    :ok = Cluster.join(pg_group)

    state = %__MODULE__{
      adapter_meta: adapter_meta,
      pg_group: pg_group,
      ready_pg_group: ready_pg_group,
      pg_ref: pg_ref,
      cluster_nodes: MapSet.new([node()])
    }

    {:ok, state, {:continue, :post_join}}
  end

  @impl true
  def handle_continue(:post_join, %{adapter_meta: adapter_meta} = state) do
    # After joining, reset the GC interval on all nodes to synchronize
    # generation rotation and prevent premature eviction of bootstrapped data.
    cluster_nodes = Cluster.pg_nodes(adapter_meta.pg_group)
    :ok = maybe_reset_gc_interval(adapter_meta, cluster_nodes)

    state = %{state | cluster_nodes: MapSet.new(cluster_nodes)}

    # Alone in the group there is nothing to receive, so this node is
    # bootstrapped and may push to whoever joins next.
    if cluster_nodes == [node()] do
      {:noreply, mark_ready(state)}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast(:bootstrap_completed, %__MODULE__{ready?: true} = state), do: {:noreply, state}
  def handle_cast(:bootstrap_completed, %__MODULE__{} = state), do: {:noreply, mark_ready(state)}

  @impl true
  def handle_info(message, state)

  # PG join event — new node(s) joined the cluster.
  # Existing nodes push their cache entries to the new node (push-based bootstrap).
  def handle_info(
        {pg_ref, :join, pg_group, pids},
        %__MODULE__{
          pg_ref: pg_ref,
          pg_group: pg_group,
          adapter_meta: adapter_meta,
          cluster_nodes: cluster_nodes
        } = state
      ) do
    # Calculate the difference between the new nodes and the cluster nodes
    new_nodes =
      pids
      |> Enum.map(&node/1)
      |> MapSet.new()
      |> MapSet.difference(cluster_nodes)

    # The pusher is elected among the nodes `:pg` knows right now, minus the
    # joiners, rather than from this process's view. The view can lag behind
    # `:pg` (events still queued, or an earlier join batch lost to a failed
    # push) and a stale view elects extra pushers; `:pg` converges as soon as
    # the connection is up.
    if MapSet.size(new_nodes) > 0 and leader?(pusher_candidates(state, new_nodes)) do
      :ok = Bootstrapper.push(adapter_meta, new_nodes)
    end

    # The view is updated regardless of what the pushes do, so a failing push
    # cannot drop the joined nodes from it.
    {:noreply, %{state | cluster_nodes: MapSet.union(cluster_nodes, new_nodes)}}
  end

  # PG leave event — node(s) left the cluster
  def handle_info(
        {pg_ref, :leave, pg_group, pids},
        %__MODULE__{pg_ref: pg_ref, pg_group: pg_group, cluster_nodes: cluster_nodes} = state
      ) do
    # Update cluster view
    updated_nodes =
      pids
      |> Enum.map(&node/1)
      |> MapSet.new()
      |> then(&MapSet.difference(cluster_nodes, &1))

    {:noreply, %{state | cluster_nodes: updated_nodes}}
  end

  # Ignore other messages
  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{pg_group: pg_group, ready_pg_group: ready_pg_group}) do
    # Leave the PG groups when the process terminates
    :ok = Cluster.leave(ready_pg_group)
    :ok = Cluster.leave(pg_group)
  end

  ## Public functions (called locally and via tests)

  @doc false
  def push_to_new_node(new_node, adapter_meta) do
    event = adapter_meta.telemetry_prefix ++ [:bootstrap]
    metadata = %{adapter_meta: adapter_meta, node: node(), peer: new_node}

    Telemetry.span(event, metadata, fn ->
      total = Replicator.push_entries(new_node, adapter_meta)

      {total, Map.put(metadata, :total, total)}
    end)
  end

  ## Private functions

  defp mark_ready(%__MODULE__{ready_pg_group: ready_pg_group} = state) do
    :ok = Cluster.join(ready_pg_group)

    %{state | ready?: true}
  end

  # The members whose own bootstrap has completed, except the nodes in the
  # join batch being handled: a joiner may well have the smallest name, but it
  # has nothing to push yet. A member still receiving its bootstrap has a
  # partial cache, so it is not a candidate either. With no such member left
  # (the cluster lost every bootstrapped node) every member is a candidate,
  # which at least makes progress; this node is always a member and never a
  # joiner to itself, so the fallback is never empty.
  defp pusher_candidates(%__MODULE__{pg_group: pg_group, ready_pg_group: ready_pg_group}, new_nodes) do
    case members_except(ready_pg_group, new_nodes) do
      [] -> members_except(pg_group, new_nodes)
      ready -> ready
    end
  end

  defp members_except(group, excluded) do
    group
    |> Cluster.pg_nodes()
    |> Enum.reject(&MapSet.member?(excluded, &1))
  end

  # Simple leader election: the candidate with the smallest name (Erlang term
  # ordering) pushes. Every node evaluates it against the same `:pg`
  # membership, so they agree as soon as `:pg` has converged. If memberships
  # temporarily diverge, the worst case is zero or multiple pushers, both
  # safe: `:put_new` is idempotent, and anti-entropy repairs any remaining
  # gaps.
  defp leader?(candidates), do: node() == Enum.min(candidates)

  defp maybe_reset_gc_interval(%{cache: cache} = adapter_meta, cluster_nodes) do
    if cache.__primary__().__adapter__() == Nebulex.Adapters.Local do
      RPC.multicall(
        cluster_nodes,
        Nebulex.Adapters.Replicated,
        :with_dynamic_cache,
        [adapter_meta, :reset_gc_interval, []],
        adapter_meta.replication_timeout
      )
    end

    :ok
  end
end
