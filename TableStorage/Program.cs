using Microsoft.Azure.Cosmos.Table;
using System;
using System.Threading.Tasks;

namespace TableStorage
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //Creating Table
            string tableName = "test11111111";
            CloudTable cloudTable =  await CreateTableAsync(tableName);

            //Inserting entity
            CustomerEntity customer = new CustomerEntity("Pink", "Panther")
            {
                Email = "Walter@contoso.in",
                PhoneNumber = "425-555-0102"
            };

            await InsertOrMergeEntityAsync(cloudTable, customer);

            //Querying the table entity
            await RetrieveEntityUsingPointQueryAsync(cloudTable, "Pink", "Panther");

        }

        public static async Task<CloudTable> CreateTableAsync(string tableName)
        {

            var connectionString = "DefaultEndpointsProtocol=https;AccountName=Your storage account name;AccountKey=Your account key;EndpointSuffix=core.windows.net";
            // Retrieve storage account information from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create a table client for interacting with the table service
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());

            Console.WriteLine("Create a Table for the demo\n");

            // Create a table client for interacting with the table service 
            CloudTable table = tableClient.GetTableReference(tableName);
            if (await table.ExistsAsync())
            {
                Console.WriteLine("Table {0} already exists \n", tableName);

            }
            else
            {
                await table.CreateAsync();
                Console.WriteLine("Table {0} created \n", tableName);

            }

            Console.WriteLine();
            return table;
        }

        public static async Task<CustomerEntity> InsertOrMergeEntityAsync(CloudTable table, CustomerEntity entity)
        {
            if (entity == null)
            {
                throw new ArgumentNullException("entity");
            }

            try
            {
                // Create the InsertOrReplace table operation
                TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge(entity);

                // Execute the operation.
                TableResult result = await table.ExecuteAsync(insertOrMergeOperation);
                CustomerEntity insertedCustomer = result.Result as CustomerEntity;

                if (result.RequestCharge.HasValue)
                {
                    Console.WriteLine("Request Charge of InsertOrMerge Operation: \n" + result.RequestCharge);
                }

                Console.WriteLine("Entity inserted successfully...\n");
                return insertedCustomer;
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        public static async Task<CustomerEntity> RetrieveEntityUsingPointQueryAsync(CloudTable table, string partitionKey, string rowKey)
        {
            try
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>(partitionKey, rowKey);
                TableResult result = await table.ExecuteAsync(retrieveOperation);
                CustomerEntity customer = result.Result as CustomerEntity;
                if (customer != null)
                {
                    Console.WriteLine("Retrieveing entities...\n");
                    Console.WriteLine("\t{0}\t{1}\t{2}\t{3}\n", customer.PartitionKey, customer.RowKey, customer.Email, customer.PhoneNumber);
                }

                if (result.RequestCharge.HasValue)
                {
                    Console.WriteLine("Request Charge of Retrieve Operation: \n" + result.RequestCharge);
                }

                return customer;
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }
    }
}
