using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;
using Newtonsoft.Json;

namespace Company.Function
{
    public class HttpTrigger1
    {
        private readonly ILogger<HttpTrigger1> _logger;

        public HttpTrigger1(ILogger<HttpTrigger1> logger)
        {
            _logger = logger;
        }

        [Function("HttpTrigger1")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req, string name)
        {
            //debugging 
            var returnValue = string.IsNullOrEmpty(name)
                ? "Hello, Azure Stream Analytics"
                : $"Hello, {name}.";
            _logger.LogInformation($"HTTP trigger function processed a request from {returnValue}.");

            // Extract the body from the request
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            _logger.LogInformation("Body received" + requestBody);
            
            // Check if the body is empty (ASA connectivity check)
            if (string.IsNullOrEmpty(requestBody))
            {
                return new StatusCodeResult(204); // 204, No Content 
            } 
            
            // Reject if too large, as per the doc
            if (requestBody.Length > 262144)
            {
                return new StatusCodeResult(413); // 413, Payload Too Large 
            }

            // Deserialize the JSON payload to a dynamic object
            dynamic? data = JsonConvert.DeserializeObject(requestBody); 
            if (data == null)
            {
                return new BadRequestObjectResult("Payload JSON inválido.");
            }
            //_logger.LogInformation("Datos JSON: " + ((object)data)?.ToString() ?? "null");

            // Connection string for Redis
            string RedisConnectionString = Environment.GetEnvironmentVariable("RedisConnectionString") 
                                           ?? throw new InvalidOperationException("RedisConnectionString environment variable is not set."); 
            // Database index for Redis
            string redisDatabaseIndexString = Environment.GetEnvironmentVariable("RedisDatabaseIndex")
                                              ?? throw new InvalidOperationException("RedisDatabaseIndex environment variable is not set.");
            int RedisDatabaseIndex = int.Parse(redisDatabaseIndexString); 

            //_logger.LogInformation("Redis connection: " + RedisConnectionString + " and Redis index " + RedisDatabaseIndex.ToString());

            using (var connection = ConnectionMultiplexer.Connect(RedisConnectionString))
            {
                _logger.LogInformation("Redis connection established.");
                // Connection refers to a property that returns a ConnectionMultiplexer
                IDatabase db = connection.GetDatabase(RedisDatabaseIndex);
                _logger.LogInformation("Redis database selected: " + RedisDatabaseIndex.ToString());
                
                // Parse items and send to binding
                foreach (var item in data)
                {
                    // Using property EventProcessedUtcTime as key
                    // and the rest of the object as value
                    string key = item.EventProcessedUtcTime;
                    //_logger.LogInformation("Procesando clave: " + key);

                    // Set the object in REdis using the key
                    db.StringSet(key, item.ToString());
                    _logger.LogInformation($"Objet set in Redis. Key: {key} - Value: {item.ToString()}");
                }

            }

            return new OkObjectResult(returnValue);
        }
    }
}
