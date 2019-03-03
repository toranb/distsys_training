defmodule Margarine.Aggregates do
  use GenServer

  require Logger

  def for(hash) do
    case :ets.lookup(:the_table, hash) do
      [{_, counter}] ->
        {:ok, counter}
      [] ->
        {:error, :not_found}
    end
  end

  def increment(hash) do
    GenServer.cast(__MODULE__, {:increment, hash})
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args \\ []) do
    :pg2.join(:margarine, self())

    :ets.new(:the_table, [:public, :named_table])
    :net_kernel.monitor_nodes(true)

    {:ok, %{}}
  end

  def handle_cast({:increment, hash}, state) do
    counter = get_counter(hash)
    current_node = Node.self()

    new_counter = Drax.GCounter.increment(counter, current_node)
    :ets.insert(:the_table, {hash, new_counter})

    members = :pg2.get_members(:margarine)
    Enum.map(members, fn (pid) ->
      GenServer.cast(pid, {:merge, hash, new_counter})
    end)

    {:noreply, state}
  end

  # :nodeup, node}, state
  #  Node.spawn(node, fn)
  #  :ets.tab2list(__MODULE__)
  #  Enum.each
  def handle_cast({:merge, hash, counter}, state) do
    local_counter = get_counter(hash)
    merged_counter = Drax.GCounter.merge(counter, local_counter)

    :ets.insert(:the_table, {hash, merged_counter})

    {:noreply, state}
  end

  defp get_counter(hash) do
    case :ets.lookup(:the_table, hash) do
      [{^hash, aggregates}] ->
        aggregates
      _ ->
        counter = Drax.GCounter.new()
        :ets.insert(:the_table, {hash, counter})
        counter
    end
  end
end

