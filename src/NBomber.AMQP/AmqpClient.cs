using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using NBomber.Contracts;
using NBomber.CSharp;

namespace NBomber.AMQP;

public class AmqpClient(IChannel channel) : IDisposable
{
    public IChannel AmqpChannel { get; } = channel;

    private readonly Channel<Response<BasicDeliverEventArgs>> _queue = Channel.CreateUnbounded<Response<BasicDeliverEventArgs>>();

    public async Task<Response<object>> Connect(string exchange, string exchangeType, string queue, string routingKey, bool durable = false,
        bool exclusive = false, bool autoDelete = false)
    {
        await AmqpChannel.ExchangeDeclareAsync(exchange: exchange, type: exchangeType);
        await AmqpChannel.QueueDeclareAsync(queue: queue, durable: durable, exclusive: exclusive, autoDelete: autoDelete);
        await AmqpChannel.QueueBindAsync(queue: queue, exchange: exchange, routingKey: routingKey);

        return Response.Ok();
    }

    public async Task<Response<object>> Subscribe(string queue, bool autoAck = true)
    {
        await AddConsumer(queue: queue, autoAck: autoAck);
        return Response.Ok();
    }

    public async Task<Response<object>> Publish<TProperties>(string exchange, string routingKey,
        TProperties basicProperties, ReadOnlyMemory<byte> body = default, bool mandatory = false)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        await AmqpChannel.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body);

        var sizeBytes = body.Length + exchange.Length + routingKey.Length +
                        GetSizeBytesOfBasicProperties(basicProperties);

        return Response.Ok(sizeBytes: sizeBytes);
    }

    public async Task<Response<object>> Publish<TProperties>(CachedString exchange, CachedString routingKey,
        TProperties basicProperties, ReadOnlyMemory<byte> body = default, bool mandatory = false)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        await AmqpChannel.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body);

        var sizeBytes = body.Length + exchange.Bytes.Length + routingKey.Bytes.Length
                        + GetSizeBytesOfBasicProperties(basicProperties);

        return Response.Ok(sizeBytes: sizeBytes);
    }

    public async Task<Response<object>> Publish<T>(PublicationAddress addr, T basicProperties,
        ReadOnlyMemory<byte> body) where T : IReadOnlyBasicProperties, IAmqpHeader
    {
        await AmqpChannel.BasicPublishAsync(addr, basicProperties, body);

        var sizeBytes = body.Length + addr.RoutingKey.Length + addr.ExchangeName.Length + addr.ExchangeType.Length
                        + GetSizeBytesOfBasicProperties(basicProperties);

        return Response.Ok(sizeBytes: sizeBytes);
    }

    public async ValueTask<Response<BasicDeliverEventArgs>> Receive(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _queue.Reader.ReadAsync(cancellationToken);
        }
        catch (OperationCanceledException ex)
        {
            throw new IgnoreMeasurementException();
        }
    }

    public async Task<Response<object>> Disconnect()
    {
        await AmqpChannel.CloseAsync();
        return Response.Ok();
    }
    
    private Task AddConsumer(string queue, bool autoAck)
    {
        var consumer = new AsyncEventingBasicConsumer(AmqpChannel);
        consumer.ReceivedAsync += (model, message) =>
        {
            var sizeBytes = GetSizeBytesOfBasicProperties(message.BasicProperties);

            sizeBytes += message.Body.Length;
            sizeBytes += message.ConsumerTag.Length;
            sizeBytes += message.RoutingKey.Length;

            _queue.Writer.TryWrite(Response.Ok(message, sizeBytes: sizeBytes));
            return Task.CompletedTask;
        };

        return AmqpChannel.BasicConsumeAsync(queue, autoAck, consumer);
    }

    private static long GetSizeBytesOfBasicProperties(IReadOnlyBasicProperties basicProperties)
    {
        var sizeBytes = 0;

        if (basicProperties.IsHeadersPresent())
        {
            sizeBytes = basicProperties.Headers!.Sum(kv =>
            {
                var result = kv.Key.Length;
                result += kv.Value is byte[] bytes ? bytes.Length : 0;
                result += kv.Value is string str ? str.Length : 0;

                return result;
            });
        }

        sizeBytes += basicProperties.Expiration?.Length ?? 0;
        sizeBytes += basicProperties.ClusterId?.Length ?? 0;
        sizeBytes += basicProperties.ContentEncoding?.Length ?? 0;
        sizeBytes += basicProperties.CorrelationId?.Length ?? 0;
        sizeBytes += basicProperties.ContentType?.Length ?? 0;
        sizeBytes += basicProperties.Type?.Length ?? 0;
        sizeBytes += basicProperties.AppId?.Length ?? 0;
        sizeBytes += basicProperties.MessageId?.Length ?? 0;
        sizeBytes += basicProperties.ReplyTo?.Length ?? 0;
        sizeBytes += basicProperties.UserId?.Length ?? 0;

        sizeBytes += basicProperties.ReplyToAddress?.ExchangeName.Length ?? 0;
        sizeBytes += basicProperties.ReplyToAddress?.RoutingKey.Length ?? 0;
        sizeBytes += basicProperties.ReplyToAddress?.ExchangeType.Length ?? 0;

        return sizeBytes;
    }

    public void Dispose()
    {
        AmqpChannel.Dispose();
    }
}