using NBomber.Contracts;
using NBomber.CSharp;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading.Channels;

namespace NBomber.AMQP;

public class AmqpClient(IChannel channel)
{
    public IChannel Channel { get; } = channel;

    private readonly Channel<Response<BasicDeliverEventArgs>> _queue =
        System.Threading.Channels.Channel.CreateUnbounded<Response<BasicDeliverEventArgs>>();

    public async Task<Response<object>> Connect(string exchange, string exchangeType, string queue, string routingKey, bool durable = false,
        bool exclusive = false, bool autoDelete = false)
    {
        await Channel.ExchangeDeclareAsync(exchange: exchange, type: exchangeType);
        await Channel.QueueDeclareAsync(queue: queue, durable: durable, exclusive: exclusive, autoDelete: autoDelete);
        await Channel.QueueBindAsync(queue: queue, exchange: exchange, routingKey: routingKey);

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
        await Channel.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body);

        var sizeBytes = body.Length + exchange.Length + routingKey.Length +
                        GetSizeBytesOfBasicProperties(basicProperties);

        return Response.Ok(sizeBytes: sizeBytes);
    }

    public async Task<Response<object>> Publish<TProperties>(CachedString exchange, CachedString routingKey,
        TProperties basicProperties, ReadOnlyMemory<byte> body = default, bool mandatory = false)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        await Channel.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body);

        var sizeBytes = body.Length + exchange.Bytes.Length + routingKey.Bytes.Length
                        + GetSizeBytesOfBasicProperties(basicProperties);

        return Response.Ok(sizeBytes: sizeBytes);
    }

    public async Task<Response<object>> Publish<T>(PublicationAddress addr, T basicProperties,
        ReadOnlyMemory<byte> body) where T : IReadOnlyBasicProperties, IAmqpHeader
    {
        await Channel.BasicPublishAsync(addr, basicProperties, body);

        var sizeBytes = body.Length + addr.RoutingKey.Length + addr.ExchangeName.Length + addr.ExchangeType.Length
                        + GetSizeBytesOfBasicProperties(basicProperties);

        return Response.Ok(sizeBytes: sizeBytes);
    }

    public async Task AddConsumer(string queue, bool autoAck)
    {
        var consumer = new AsyncEventingBasicConsumer(Channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var sizeBytes = GetSizeBytesOfBasicProperties(ea.BasicProperties);

            sizeBytes += ea.Body.Length;
            sizeBytes += ea.ConsumerTag.Length;
            sizeBytes += ea.RoutingKey.Length;

            if (ea.BasicProperties.Headers != null && ea.BasicProperties.Headers.ContainsKey("timestamp"))
            {
                var timestampMs = (int)ea.BasicProperties.Headers["timestamp"];
                var latency = DateTime.UtcNow.Millisecond - timestampMs;

                await _queue.Writer.WriteAsync(Response.Ok(ea, sizeBytes: sizeBytes, customLatencyMs: latency));
            }
            else
                await _queue.Writer.WriteAsync(Response.Ok(ea, sizeBytes: sizeBytes));             
        };

        await Channel.BasicConsumeAsync(queue, autoAck, consumer);
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

    public ValueTask<Response<BasicDeliverEventArgs>> Receive() => _queue.Reader.ReadAsync();

    public async Task<Response<object>> Disconnect()
    {
        await Channel.CloseAsync();
        return Response.Ok();
    }
}