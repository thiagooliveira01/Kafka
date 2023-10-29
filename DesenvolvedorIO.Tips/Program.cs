using Confluent.Kafka;
using System.Text;

const string Topico = "desenvolvedor.io";

var i = 1;
//for (i = 1; i <= 5; i++)
//{
//    await Produzir(i);
//}

//_ = Task.Run(() => Consumir("consumidor1", AutoOffsetReset.Earliest));
//_ = Task.Run(() => Consumir("grupo2", AutoOffsetReset.Earliest));
//_ = Task.Run(() => Consumir("grupo3", AutoOffsetReset.Earliest));
_ = Task.Run(() => Consumir("grupo4", AutoOffsetReset.Latest));


while (true)
{
    Console.ReadLine();
    Produzir(i).GetAwaiter().GetResult();
    i++;
}
static async Task Produzir(int i)
{
    var configuracao = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",

        // Habilitar idempotência
        EnableIdempotence = true,
        Acks = Acks.All,
        MaxInFlight = 1,
        MessageSendMaxRetries = 2,

        TransactionalId = Guid.NewGuid().ToString()
    };

    var key = Guid.NewGuid().ToString();
    var mensagem = $"Mensagem ( {i} ) KEY: {key}";

    Console.WriteLine(">> Enviada:\t " + mensagem);

    try
    {
        // Instância
        using var producer = new ProducerBuilder<string, string>(configuracao).Build();

        //Iniciar uma transação
        producer.InitTransactions(TimeSpan.FromSeconds(5));
        producer.BeginTransaction();

        var headers = new Headers();
        headers.Add("application", Encoding.UTF8.GetBytes("payment"));
        headers.Add("transactionId", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));

        var result = await producer.ProduceAsync(Topico, new Message<string, string>
        {
            Key = key,
            Value = mensagem,
            Headers = headers
        });
        // Enviar Mensagem 2
        // Enviar Mensagem 3
        // Atualizar status no banco de dados

        // Confirma a transação
        producer.CommitTransaction();

        // Em caso de erro pode abortar a transação
        //producer.AbortTransaction();

        await Task.CompletedTask;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.ToString());
    }
}

static void Consumir(string consumerId, AutoOffsetReset autoOffsetReset)
{
    var clientId = Guid.NewGuid().ToString().Substring(0, 5);

    var conf = new ConsumerConfig
    {
        ClientId = clientId,
        GroupId = consumerId,
        BootstrapServers = "localhost:9092",
        AutoOffsetReset = autoOffsetReset,
        EnablePartitionEof = true,
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false,
    };

    using var consumer = new ConsumerBuilder<string, string>(conf).Build();

    consumer.Subscribe(Topico);

    while (true)
    {
        var result = consumer.Consume();

        if (result.IsPartitionEOF)
        {
            continue;
        }

        var headers = result
            .Message
            .Headers
            .ToDictionary(p => p.Key, p => Encoding.UTF8.GetString(p.GetValueBytes()));

        var application = headers["application"];
        var transactionId = headers["transactionId"];

        var messsage = "<< Recebida: \t" + result.Message.Value;
        Console.WriteLine(messsage);

        consumer.Commit(result);
        consumer.StoreOffset(result.TopicPartitionOffset);
    }
}

#region Aula Consumir mensagens mais de um vez
//static void Consumir(string consumerId, AutoOffsetReset autoOffsetReset)
//{
//    var clientId = Guid.NewGuid().ToString().Substring(0, 5);

//    var conf = new ConsumerConfig
//    {
//        ClientId = clientId,
//        GroupId = consumerId,
//        BootstrapServers = "localhost:9092",
//        AutoOffsetReset = autoOffsetReset,
//        EnablePartitionEof = true,
//        EnableAutoCommit = false,
//        EnableAutoOffsetStore = false,

//        // Configurar para consumir apenas mensagens confirmadas.
//        IsolationLevel = IsolationLevel.ReadCommitted,
//    };

//    using var consumer = new ConsumerBuilder<string, string>(conf).Build();

//    consumer.Subscribe(Topico);

//    int Tentativas = 0;

//    while (true)
//    {
//        var result = consumer.Consume();

//        if (result.IsPartitionEOF)
//        {
//            continue;
//        }

//        //var messsage = "<< Recebida: \t" + result.Message.Value + $" - {consumerId}-{autoOffsetReset}-{clientId}";
//        var messsage = "<< Recebida: \t" + result.Message.Value;
//        Console.WriteLine(messsage);

//        // Tentar processar mensagem
//        Tentativas++; 
//        if (!ProcessarMensagem(result) && Tentativas < 3)
//        { 
//            consumer.Seek(result.TopicPartitionOffset);

//            continue;
//        }

//        if(Tentativas > 1)
//        {
//            // Publicar mensagem em uma fila para analise!
//            Console.WriteLine("Enviando mensagem para: DeadLetter");
//            Tentativas = 0;
//        }

//        consumer.Commit(result);
//        consumer.StoreOffset(result.TopicPartitionOffset); 
//    }
//}
#endregion

static bool ProcessarMensagem(ConsumeResult<string, string> result)
{
    Console.WriteLine($"KEY:{result.Message.Key} - {DateTime.Now}");
    Task.Delay(2000).Wait();
    return false;
}