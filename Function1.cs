using System;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Microsoft.Data.SqlClient;
namespace TicketAppFunction
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(Function1))]
        public async Task Run([QueueTrigger("purchase", Connection = "AzureWebJobsStorage")] QueueMessage message)
        {
            _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");

            //deserialize
            string messageJson = message.MessageText;
            var JSONoptions = new JsonSerializerOptions
            {   
                PropertyNameCaseInsensitive = true
            };

            CustomerPurchase purchase = JsonSerializer.Deserialize<CustomerPurchase>(messageJson, JSONoptions);
        
            if(purchase == null)
            {
                _logger.LogError("failed to deserialize" + message.MessageText);
                return;
            }

            _logger.LogInformation("Purchase: " + purchase);

            //add to DB

            // get connection string from app settings
            string? connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("SQL connection string is not set in the environment variables.");
            }

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync(); // Note the ASYNC

                var query = "INSERT INTO CustomerPurchase (ConcertId, Email, Name, Phone, Quantity, CreditCard, Expiration, SecurityCode, Address, City, Province, PostalCode, Country) VALUES (@ConcertId, @Email, @Name, @Phone, @Quantity, @CreditCard, @Expiration, @SecurityCode, @Address, @City, @Province, @PostalCode, @Country)";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@ConcertId", purchase.ConcertId);
                    cmd.Parameters.AddWithValue("@Email", purchase.Email);
                    cmd.Parameters.AddWithValue("@Name", purchase.Name);
                    cmd.Parameters.AddWithValue("@Phone", purchase.Phone);
                    cmd.Parameters.AddWithValue("@Quantity", purchase.Quantity);
                    cmd.Parameters.AddWithValue("@CreditCard", purchase.CreditCard);
                    cmd.Parameters.AddWithValue("@Expiration", purchase.Expiration);
                    cmd.Parameters.AddWithValue("@SecurityCode", purchase.SecurityCode);
                    cmd.Parameters.AddWithValue("@Address", purchase.Address);
                    cmd.Parameters.AddWithValue("@City", purchase.City);
                    cmd.Parameters.AddWithValue("@Province", purchase.Province);
                    cmd.Parameters.AddWithValue("@PostalCode", purchase.PostalCode);
                    cmd.Parameters.AddWithValue("@Country", purchase.Country);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }
    }
}
