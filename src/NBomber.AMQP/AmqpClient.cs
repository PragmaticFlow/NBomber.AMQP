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
   
   public void AddConsumer(string queue, bool autoAck)
   {
      var consumer = new EventingBasicConsumer(Channel);
      consumer.Received += (model, ea) =>
      {
         var sizeBytes = ea.Body.Length;
         
         var headersLength = 0;
         if (ea.BasicProperties.IsHeadersPresent())
         {
            headersLength = ea.BasicProperties.Headers!.Sum(kv =>
            {
               var result = kv.Key.Length;
               result += kv.Value is byte[] bytes ? bytes.Length : 0;
               result += kv.Value is string str ? str.Length : 0;
               
               return result;
            });
         }

         sizeBytes += headersLength;
         sizeBytes += ea.ConsumerTag.Length;
         sizeBytes += ea.RoutingKey.Length;
         
         sizeBytes += ea.BasicProperties.Expiration?.Length ?? 0;
         sizeBytes += ea.BasicProperties.Type?.Length ?? 0;
         sizeBytes += ea.BasicProperties.AppId?.Length ?? 0;
         sizeBytes += ea.BasicProperties.ClusterId?.Length ?? 0;
         sizeBytes += ea.BasicProperties.ContentEncoding?.Length ?? 0;
         sizeBytes += ea.BasicProperties.ContentType?.Length ?? 0;
         sizeBytes += ea.BasicProperties.CorrelationId?.Length ?? 0;
         sizeBytes += ea.BasicProperties.MessageId?.Length ?? 0;
         sizeBytes += ea.BasicProperties.ReplyTo?.Length ?? 0;
         sizeBytes += ea.BasicProperties.UserId?.Length ?? 0;
         
         _queue.Writer.WriteAsync(Response.Ok(payload: ea, sizeBytes: sizeBytes));
      };

      Channel.BasicConsume(queue: queue, autoAck: autoAck, consumer: consumer);
   }

   public ValueTask<Response<BasicDeliverEventArgs>> Receive() => _queue.Reader.ReadAsync();
}