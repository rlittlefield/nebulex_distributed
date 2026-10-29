defmodule Nebulex.Adapters.Replicated.Bootstrapper do
  @moduledoc false

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapters.Replicated.ClusterMonitor
  alias Nebulex.Distributed.{Cluster, RPC}

  # State
  defstruct [
    :adapter_meta,
    :task_supervisor,
    queue: :queue.new(),
    queued: MapSet.new(),
    in_flight: nil
  ]

  ## API

  @doc false
  def start_link(adapter_meta) do
    GenServer.start_link(__MODULE__, adapter_meta, name: server_name(adapter_meta))
  end

  @doc false
  @spec server_name(map()) :: atom()
  def server_name(%{name: name}) do
    camelize_and_concat([name, "Bootstrapper"])
  end

  @doc false
  @spec task_supervisor_name(map()) :: atom()
  def task_supervisor_name(%{name: name}) do
    camelize_and_concat([name, "BootstrapTasks"])
  end

  @doc false
  @spec push(map(), Enumerable.t()) :: :ok
  def push(adapter_meta, nodes) do
    GenServer.cast(server_name(adapter_meta), {:push, Enum.to_list(nodes)})
  end

  ## GenServer Callbacks

  @impl true
  def init(adapter_meta) do
    state = %__MODULE__{
      adapter_meta: adapter_meta,
      task_supervisor: task_supervisor_name(adapter_meta)
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:push, nodes}, state) do
    state = Enum.reduce(nodes, state, &enqueue/2)

    {:noreply, maybe_push_next(state)}
  end

  @impl true
  def handle_info(message, state)

  # The push finished; tell the target it is bootstrapped, then take the next one
  def handle_info({ref, _total}, %__MODULE__{in_flight: {node, ref}} = state) do
    _ = Process.demonitor(ref, [:flush])
    :ok = notify_completed(node, state.adapter_meta)

    {:noreply, maybe_push_next(%{state | in_flight: nil})}
  end

  # The push failed. The task logged it and the `:bootstrap` span reported it.
  # The target is not told the bootstrap completed, so it stays out of the
  # pusher candidates.
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %__MODULE__{in_flight: {_node, ref}} = state
      ) do
    {:noreply, maybe_push_next(%{state | in_flight: nil})}
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end

  ## Private functions

  defp enqueue(node, %__MODULE__{queue: queue, queued: queued} = state) do
    if MapSet.member?(queued, node) do
      state
    else
      %{state | queue: :queue.in(node, queue), queued: MapSet.put(queued, node)}
    end
  end

  # One push at a time, in arrival order, whatever batches the joins came in
  defp maybe_push_next(%__MODULE__{in_flight: nil} = state) do
    case :queue.out(state.queue) do
      {{:value, node}, queue} ->
        state = %{state | queue: queue, queued: MapSet.delete(state.queued, node)}

        if node in Cluster.pg_nodes(state.adapter_meta.pg_group) do
          start_push(node, state)
        else
          # The node left before its turn
          maybe_push_next(state)
        end

      {:empty, _queue} ->
        state
    end
  end

  defp maybe_push_next(%__MODULE__{} = state) do
    state
  end

  # The push runs in an unlinked task: a failure surfaces as a `:DOWN` message
  # here, in the task's log, and in the `:bootstrap` span, not as a crash of
  # this process and its queue.
  defp start_push(
         node,
         %__MODULE__{task_supervisor: task_supervisor, adapter_meta: adapter_meta} = state
       ) do
    task =
      Task.Supervisor.async_nolink(task_supervisor, ClusterMonitor, :push_to_new_node, [
        node,
        adapter_meta
      ])

    %{state | in_flight: {node, task.ref}}
  end

  defp notify_completed(node, adapter_meta) do
    _ =
      RPC.call(
        node,
        ClusterMonitor,
        :bootstrap_completed,
        [adapter_meta],
        adapter_meta.replication_timeout
      )

    :ok
  end
end
