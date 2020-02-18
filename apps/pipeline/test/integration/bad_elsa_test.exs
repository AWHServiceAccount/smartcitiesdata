defmodule BadElsaTest do
  use ExUnit.Case
  use Divo, services: [:kafka, :zookeeper]

  import SmartCity.TestHelper

  @endpoints Application.get_env(:pipeline, :elsa_brokers)

  test "non direct ACKs duplicate data" do
    setup_topic("bad_elsa")
    test_pid = self()
    options = elsa_options("bad_elsa", test_pid, false)

    {:ok, first_run_pid} = start_consumer_group(options)
    wait_for_messages_on_test_pid(test_pid)
    stop_consumer_group(first_run_pid)

    {:ok, _second_run_pid} = start_consumer_group(options)
    Process.sleep(10_000) # give it time to pull in duplicates if they are there

    eventually(fn ->
      assert {:message_queue_len, 2} == Process.info(test_pid, :message_queue_len)
    end)
  end

  test "direct ACKs do not duplicate data" do
    setup_topic("good_elsa")
    test_pid = self()
    options = elsa_options("good_elsa", test_pid, true)

    {:ok, first_run_pid} = start_consumer_group(options)
    wait_for_messages_on_test_pid(test_pid)
    stop_consumer_group(first_run_pid)

    {:ok, _second_run_pid} = start_consumer_group(options)
    Process.sleep(10_000) # give it time to pull in duplicates if they are there

    eventually(fn ->
      assert {:message_queue_len, 1} == Process.info(test_pid, :message_queue_len)
    end)
  end

  defp setup_topic(topic) do
    Elsa.create_topic(@endpoints,topic)

    eventually(fn ->
      Elsa.topic?(@endpoints, topic)
    end)

    Elsa.produce(@endpoints, topic, ["a", "b", "c"])
  end

  defp start_consumer_group(options) do
    {:ok, pid} = Elsa.Supervisor.start_link(options)
    IO.inspect(pid, label: "starting")
    {:ok, pid}
  end

  def stop_consumer_group(pid) do
    Process.exit(pid, :normal) # wait for the messages to have been processed, then give it a clean exit. watch for the "acking" log message to show that it does drain
    IO.inspect(pid, label: "stopped")
    Process.sleep(10_000) # give it time to die
  end

  defp wait_for_messages_on_test_pid(test_pid) do
    eventually(fn ->
      {:message_queue_len, 1} == Process.info(test_pid, :message_queue_len)
    end)
  end

  defp elsa_options(topic, test_pid, direct_ack_enabled) do
    [
      name: :"#{topic}_consumer",
      endpoints: @endpoints,
      connection: :"#{topic}",
      group_consumer: [
        group: "test-#{topic}",
        topics: [topic],
        handler: FakeMessageHandler,
        handler_init_args: [test_pid: test_pid],
        config: [
          begin_offset: :earliest,
          offset_reset_policy: :reset_to_earliest,
          max_bytes: 1_000_000,
          min_bytes: 0,
          max_wait_time: 10_000
        ],
        direct_ack: direct_ack_enabled
      ]
    ]
  end
end

defmodule FakeMessageHandler do
  use Elsa.Consumer.MessageHandler

  def init(args) do
    {:ok, %{test_pid: Keyword.fetch!(args, :test_pid)}}
  end

  def handle_messages(messages, %{test_pid: test_pid}) do
    count = Enum.count(messages)
    IO.inspect(count, label: "processing")
    send(test_pid, {:processing, messages})
    Process.sleep(5_000) # pretend we're doing some heavy lifting here
    IO.inspect(count, label: "acking")
    {:ack, %{test_pid: test_pid}}
  end
end
