using System.Threading.Channels;
using NBomber.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using NBomber.CSharp;

namespace NBomber.AMQP;

public class AmqpClient(IChannel channel)
{
   public IChannel Channel { get; } = channel;

   private readonly Channel<Response<BasicDeliverEventArgs>> _queue = 
      System.Threading.Channels.Channel.CreateUnbounded<Response<BasicDeliverEventArgs>>();

   public Response<ReadOnlyMemory<byte>> Publish<TProperties>(string exchange, string routingKey, 
      in TProperties basicProperties, ReadOnlyMemory<byte> body = default, bool mandatory = false)
      where TProperties : IReadOnlyBasicProperties, IAmqpHeader
   {
       Channel.BasicPublishAsync(exchange, routingKey, basicProperties, body, mandatory);

       var sizeBytes = body.Length + exchange.Length + routingKey.Length +
                       GetSizeBytesOfBasicProperties(basicProperties);
      
       return Response.Ok(payload: body, sizeBytes: sizeBytes);
   }
   
   public Response<ReadOnlyMemory<byte>> Publish<TProperties>(CachedString exchange, CachedString routingKey, 
      in TProperties basicProperties, ReadOnlyMemory<byte> body = default, bool mandatory = false)
      where TProperties : IReadOnlyBasicProperties, IAmqpHeader
   {
      Channel.BasicPublishAsync(exchange, routingKey, basicProperties, body, mandatory);

      var sizeBytes = body.Length + exchange.Bytes.Length + routingKey.Bytes.Length 
                      + GetSizeBytesOfBasicProperties(basicProperties);
      
      return Response.Ok(payload: body, sizeBytes: sizeBytes);
   }
   
   public Response<ReadOnlyMemory<byte>> Publish<T>(PublicationAddress addr, in T basicProperties,
      ReadOnlyMemory<byte> body) where T : IReadOnlyBasicProperties, IAmqpHeader
   {
      Channel.BasicPublishAsync(addr, basicProperties, body);

      var sizeBytes = body.Length + addr.RoutingKey.Length + addr.ExchangeName.Length + addr.ExchangeType.Length 
                      + GetSizeBytesOfBasicProperties(basicProperties);
      
      return Response.Ok(payload: body, sizeBytes: sizeBytes);
   }
   
   public void AddConsumer(string queue, bool autoAck)
   {
      var consumer = new EventingBasicConsumer(Channel);
      consumer.Received += (model, ea) =>
      {
         var sizeBytes = GetSizeBytesOfBasicProperties(ea.BasicProperties);

         sizeBytes += ea.Body.Length;
         sizeBytes += ea.ConsumerTag.Length;
         sizeBytes += ea.RoutingKey.Length;
         
         _queue.Writer.WriteAsync(Response.Ok(payload: ea, sizeBytes: sizeBytes));
      };

       Channel.BasicConsume(queue: queue, autoAck: autoAck, consumer: consumer);
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
}