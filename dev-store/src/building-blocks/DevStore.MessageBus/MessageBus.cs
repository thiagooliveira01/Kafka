using Confluent.Kafka;
using DevStore.Core.Messages.Integration;
using DevStore.MessageBus.Serializador;
using NetDevPack.OpenTelemetry.Extensions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DevStore.MessageBus
{
    public class MessageBus : IMessageBus
    { 
        private readonly string _bootstrapserver;

        public MessageBus(string bootstrapserver)
        {
            _bootstrapserver = bootstrapserver;
        }

        public async Task ProducerAsync<T>(string topic, T message) where T : IntegrationEvent
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapserver,
            };

            //var payload = System.Text.Json.JsonSerializer.Serialize(message);

            var headers = new Dictionary<string, string>();
            headers["transactionId"] = Guid.NewGuid().ToString();
            var activity = NetDevPackExtensions.StartProducer(headers, $"Producer {topic}");

            var producer = new ProducerBuilder<string, T>(config)
                .SetValueSerializer(new SerializerDevStore<T>())
                .Build();

            var result = await producer.ProduceAsync(topic, new Message<string, T>
            {
                Key = Guid.NewGuid().ToString(),
                Value = message,
                Headers = headers.DictionaryToHeader()
            });

            await Task.CompletedTask;
        }


        public async Task ConsumerAsync<T>(
            string topic, 
            Func<T, Task> onMessage,
            CancellationToken cancellation) where T : IntegrationEvent
        {
            _ = Task.Factory.StartNew(async () =>
            {
                var config = new ConsumerConfig
                {
                    GroupId = "grupo-curso",
                    BootstrapServers = _bootstrapserver,
                    EnableAutoCommit = false,
                    EnablePartitionEof = true,
                };

                using var consumer = new ConsumerBuilder<string, T>(config)
                    .SetValueDeserializer(new DeserializerDevStore<T>())
                    .Build();

                consumer.Subscribe(topic);

                while(!cancellation.IsCancellationRequested)
                {
                    var result = consumer.Consume();

                    if(result.IsPartitionEOF)
                    {
                        continue;
                    }

                    var headers = result.Message.Headers.HeaderToDictionary();
                    NetDevPackExtensions.StartConsumer(headers, $"Consumidor: {topic}");
                    //var message = System.Text.Json.JsonSerializer.Deserialize<T>(result.Message.Value);

                    await onMessage(result.Message.Value);

                    consumer.Commit();
                }
            }, cancellation, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            
            await Task.CompletedTask;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

   
    }
}