using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace Sample.DurableFn
{
    public static class ReIngestionOrchestration
    {
        [Function("ReIngestionOrchestration_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("ReIngestionOrchestration_HttpStart");

            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(ReIngestionOrchestration));

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }

        [Function(nameof(ReIngestionOrchestration))]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(ReIngestionOrchestration));
            logger.LogInformation("Starting reingestion orchestration.");
            
            var items = await context.CallActivityAsync<List<string>>(nameof(GetItemsToReingest));

            var batchSize = 5;
            for (int batchIndex = 0; batchIndex < items.Count; batchIndex += batchSize)
            {
                var batch = items.Skip(batchIndex).Take(batchSize).ToList();
                logger.LogInformation($"Processing batch {batchIndex / batchSize + 1} of {Math.Ceiling((double)items.Count / batchSize)}.");

                var tasks = new List<Task>();
                foreach (var item in batch)
                {
                    tasks.Add(context.CallActivityAsync(nameof(ProcessItem), item));
                }

                await Task.WhenAll(tasks);
                logger.LogInformation($"Finished processing batch {batchIndex / batchSize + 1} of {Math.Ceiling((double)items.Count / batchSize)}.");
            }
            
            return;
        }

        [Function(nameof(GetItemsToReingest))]
        public static async Task<List<string>> GetItemsToReingest([ActivityTrigger] FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger(nameof(GetItemsToReingest));
            logger.LogInformation("Getting items to reingest.");

            var items = new List<string>();
            foreach (var item in Enumerable.Range(1, 101))
            {
                items.Add(Guid.NewGuid().ToString());
            }

            return await Task.FromResult(items);
        }

        [Function(nameof(ProcessItem))]
        public static async Task ProcessItem([ActivityTrigger] string item, FunctionContext context)
        {
            ILogger logger = context.GetLogger(nameof(ProcessItem));
            logger.LogInformation($"Processing item {item}.");

            // Simulate processing time
            await Task.Delay(TimeSpan.FromSeconds(2));
        }
    }
}
