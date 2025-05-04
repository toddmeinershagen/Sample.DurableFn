using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace Sample.DurableFn.FanOutIn
{
    public class Document
    {
        public required string Id { get; set; }
        public required string Title { get; set; }
        public required string ContentType { get; set; }
        public DateTime CreatedDate { get; set; }
        public required string Author { get; set; }
        public int PageCount { get; set; }
    }

    public static class ReIngestionOrchestrator
    {
        [Function("ReIngestionOrchestration_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(nameof(ReIngestionOrchestrator));

            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(nameof(ReIngestionOrchestrator));
            
            logger.LogDebug($"Started orchestration with ID = '{instanceId}'.");

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }

        [Function(nameof(ReIngestionOrchestrator))]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            var logger = context.CreateReplaySafeLogger("ReIngestionOrchestrator");
            logger.LogDebug("Starting reingestion orchestration.");
            
            var documents = await context.CallActivityAsync<List<Document>>(nameof(GetDocumentsAsync));

            var batchSize = 5;
            for (int batchIndex = 0; batchIndex < documents.Count; batchIndex += batchSize)
            {
                var batch = documents.Skip(batchIndex).Take(batchSize).ToList();
                logger.LogDebug($"Processing batch {batchIndex / batchSize + 1} of {Math.Ceiling((double)documents.Count / batchSize)}.");

                var tasks = new List<Task>();
                foreach (var document in batch)
                {
                    tasks.Add(context.CallActivityAsync(nameof(ProcessDocumentAsync), document));
                }

                // Wait for all batch of documents to complete
                await Task.WhenAll(tasks);
                logger.LogDebug($"Finished processing batch {batchIndex / batchSize + 1} of {Math.Ceiling((double)documents.Count / batchSize)}.");
            }

            // Optional: Run a final activity after all batches are processed
            await context.CallActivityAsync(nameof(SendCompletionNotificationAsync), documents.Count);
        }

        [Function(nameof(GetDocumentsAsync))]
        public static async Task<List<Document>> GetDocumentsAsync([ActivityTrigger] FunctionContext executionContext)
        {
           var logger = executionContext.GetLogger(nameof(ReIngestionOrchestrator));
            logger.LogDebug("Getting document list");

            var documents = new List<Document>();
            string[] contentTypes = { "pdf", "docx", "txt", "xlsx", "html" };

            var randomizer = new Random();

            foreach (var index in Enumerable.Range(1, 23))
            {
                documents.Add(new Document
                {
                    Id = $"doc-{index:D5}",
                    Title = $"Document {index}",
                    ContentType = contentTypes[randomizer.Next(contentTypes.Length)],
                    CreatedDate = DateTime.Now.AddDays(-randomizer.Next(30)),
                    Author = $"Author {randomizer.Next(1, 10)}",
                    PageCount = randomizer.Next(1, 100)
                });
            }

            logger.LogDebug($"Retrieved {documents.Count} documents");
            return await Task.FromResult(documents);
        }

        [Function(nameof(ProcessDocumentAsync))]
        public static async Task ProcessDocumentAsync([ActivityTrigger] Document document, FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(nameof(ReIngestionOrchestrator));
            logger.LogDebug($"Processing document {document.Id}: {document.Title} ({document.ContentType})");

            // Simulate different processing times based on document size
            var processingTime = TimeSpan.FromSeconds(2) + TimeSpan.FromMilliseconds(document.PageCount * 10); // More pages = more processing time
            await Task.Delay(processingTime);

            logger.LogDebug($"Completed processing document {document.Id}");
        }

        [Function(nameof(SendCompletionNotificationAsync))]
        public static void SendCompletionNotificationAsync([ActivityTrigger] int documentCount, FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger(nameof(ReIngestionOrchestrator));
            logger.LogDebug($"All {documentCount} documents have been processed successfully");
            
            // Here you could:
            // - Send an email notification
            // - Update a status dashboard
            // - Generate a processing report
            // - Trigger downstream processes
            // - etc.
        }
    }
}
