using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using NBomber.Contracts;
using NBomber.CSharp;

namespace NBomber.AMQP;

/// <summary>
/// Represents an AMQP client that provides methods for declaring queues, publishing messages,
/// subscribing to queues, and receiving messages. Wraps an underlying AMQP channel.
/// </summary>
public class AmqpClient(IChannel channel) : IDisposable
{
    private readonly Channel<Response<BasicDeliverEventArgs>> _queue = Channel.CreateUnbounded<Response<BasicDeliverEventArgs>>();
    private long _msgReceivedCount;

    /// <summary>
    /// Gets the underlying AMQP channel used for communication with the message broker.
    /// </summary>
    public IChannel AmqpChannel { get; } = channel;

    /// <summary>
    /// Gets the total number of messages received by the client.
    /// </summary>
    public long MsgReceivedCount => _msgReceivedCount;

    /// <summary>
    /// Declares an AMQP exchange and queue, then binds the queue to the exchange using the specified routing key.
    /// </summary>
    /// <param name="exchange">The name of the exchange.</param>
    /// <param name="exchangeType">The type of the exchange (e.g., "direct", "topic").</param>
    /// <param name="queue">The name of the queue to declare.</param>
    /// <param name="routingKey">The routing key to use for binding.</param>
    /// <param name="durable">Whether the queue should survive broker restarts.</param>
    /// <param name="exclusive">Whether the queue is exclusive to the connection.</param>
    /// <param name="autoDelete">Whether the queue should be automatically deleted when unused.</param>
    /// <returns>A response indicating success or failure.</returns>
    public async Task<Response<object>> DeclareQueue(string exchange, string exchangeType, string queue, string routingKey, bool durable = false,
        bool exclusive = false, bool autoDelete = false)
    {
        await AmqpChannel.ExchangeDeclareAsync(exchange: exchange, type: exchangeType);
        await AmqpChannel.QueueDeclareAsync(queue: queue, durable: durable, exclusive: exclusive, autoDelete: autoDelete);
        await AmqpChannel.QueueBindAsync(queue: queue, exchange: exchange, routingKey: routingKey);

        return Response.Ok();
    }

    /// <summary>
    /// Subscribes to the specified AMQP queue by adding a consumer.
    /// </summary>
    /// <param name="queue">The name of the queue to subscribe to.</param>
    /// <param name="autoAck">Whether to automatically acknowledge messages.</param>
    /// <returns>A response indicating success or failure.</returns>
    public async Task<Response<object>> Subscribe(string queue, bool autoAck = true)
    {
        await AddConsumer(queue: queue, autoAck: autoAck);
        return Response.Ok();
    }

    /// <summary>
    /// Publishes a message to the specified AMQP exchange using the given routing key.
    /// </summary>
    /// <param name="exchange">The name of the exchange to publish to.</param>
    /// <param name="routingKey">The routing key for the message.</param>
    /// <param name="body">The message payload.</param>
    /// <param name="mandatory">Whether the message must be routed to a queue.</param>
    /// <returns>A response indicating success and size in bytes.</returns>
    public async Task<Response<object>> Publish(string exchange, string routingKey,
        ReadOnlyMemory<byte> body = default, bool mandatory = false)
    {
        await AmqpChannel.BasicPublishAsync(exchange, routingKey, mandatory, body);

        var sizeBytes = body.Length + exchange.Length + routingKey.Length;

        return Response.Ok(sizeBytes: sizeBytes);
    }

    /// <summary>
    /// Publishes a message with custom AMQP properties to the specified exchange using the given routing key.
    /// </summary>
    /// <typeparam name="TProperties">The type of the message properties implementing AMQP interfaces.</typeparam>
    /// <param name="exchange">The name of the exchange.</param>
    /// <param name="routingKey">The routing key for the message.</param>
    /// <param name="basicProperties">Custom properties for the AMQP message.</param>
    /// <param name="body">The message body.</param>
    /// <param name="mandatory">Whether the message must be routed to a queue.</param>
    /// <returns>A response indicating success and size in bytes.</returns>
    public async Task<Response<object>> Publish<TProperties>(string exchange, string routingKey,
        TProperties basicProperties, ReadOnlyMemory<byte> body = default, bool mandatory = false)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        await AmqpChannel.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body);

        var sizeBytes = body.Length + exchange.Length + routingKey.Length +
                        GetSizeBytesOfBasicProperties(basicProperties);

        return Response.Ok(sizeBytes: sizeBytes);
    }

    /// <summary>
    /// Asynchronously receives an AMQP message.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token to cancel the receive operation.</param>
    /// <returns>The received message or an exception if cancelled.</returns>
    public async ValueTask<Response<BasicDeliverEventArgs>> Receive(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _queue.Reader.ReadAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw new IgnoreMeasurementException();
        }
    }

    /// <summary>
    /// Gracefully closes the AMQP channel and disconnects from the broker.
    /// </summary>
    /// <returns>A response indicating the result of the disconnect operation.</returns>
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
            Interlocked.Increment(ref _msgReceivedCount);
            
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

    /// <summary>
    /// Releases resources used by the current instance, including the underlying AMQP channel.
    /// </summary>
    public void Dispose()
    {
        AmqpChannel.Dispose();
    }
}