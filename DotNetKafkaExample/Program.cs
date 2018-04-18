using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetKafkaExample
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("require bootstrap servers");
                return;
            }

            Console.WriteLine("start. Ctl+C to exit.");

            // Kafkaサーバーアドレス
            string bootstrapServers = args[0];

            // Taskキャンセルトークン
            var tokenSource = new CancellationTokenSource();

            // プロデューサータスク
            var pTask = Task.Run(() => new Action<string, CancellationToken>(async (bs, cancel) =>
            {
                var cf = new Dictionary<string, object> {
                    { "bootstrap.servers", bs }
                };

                using (var producer = new Producer<string, string>(cf, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
                {
                    while (true)
                    {
                        if (cancel.IsCancellationRequested)
                        {
                            break;
                        }

                        var timestamp = DateTime.UtcNow.ToBinary();

                        var pa = producer.ProduceAsync("test.C", timestamp.ToString(), JsonConvert.SerializeObject(new SendMessage
                        {
                            Message = "Hello",
                            Timestamp = timestamp
                        }));

                        await pa.ContinueWith(t => Console.WriteLine($"success send. message: {t.Result.Value}"));
                        await Task.Delay(10000);
                    }

                    // 停止前処理
                    producer.Flush(TimeSpan.FromMilliseconds(10000));
                }
            })(bootstrapServers, tokenSource.Token), tokenSource.Token);

            // コンシューマータスク
            var cTask = Task.Run(() => new Action<string, CancellationToken>((bs, cancel) =>
            {
                var cf = new Dictionary<string, object> {
                    { "bootstrap.servers", bs },
                    { "group.id", "test" },
                    { "enable.auto.commit", false },
                    { "auto.commit.interval.ms", 5000 },
                    { "statistics.interval.ms", 60000 },
                    { "default.topic.config", new Dictionary<string, object>()
                        {
                            { "auto.offset.reset", "smallest" }
                        }
                    }
                };

                using (var consumer = new Consumer<string, string>(cf, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
                {
                    consumer.OnError += (_, error) => Console.WriteLine($"consumer error. reason: {error.Reason}");

                    consumer.OnConsumeError += (_, error) => Console.WriteLine($"fail consume. reason: {error.Error}");

                    consumer.OnPartitionsAssigned += (_, partitions) => consumer.Assign(partitions);

                    consumer.OnPartitionsRevoked += (_, partitions) => consumer.Unassign();

                    consumer.Subscribe("test.C");

                    while (true)
                    {
                        if (cancel.IsCancellationRequested)
                        {
                            break;
                        }

                        Message<string, string> msg;
                        if (!consumer.Consume(out msg, TimeSpan.FromMilliseconds(100)))
                        {
                            continue;
                        }

                        var cm = JsonConvert.DeserializeObject<ConsumedMessage>(msg.Value);
                        Console.WriteLine($"success consumed. message: {cm.Message}, timestamp: {cm.Timestamp}");

                        consumer.CommitAsync(msg);
                    }
                }
            })(bootstrapServers, tokenSource.Token), tokenSource.Token);

            // Ctl+C待機
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                tokenSource.Cancel();
            };

            Task.WaitAll(pTask, cTask);

            Console.WriteLine("stop. press any key to close.");

            Console.ReadKey();
        }
    }
}
