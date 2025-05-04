namespace Sample.DurableFn.FanOutIn;

public class Document
    {
        public required string Id { get; set; }
        public required string Title { get; set; }
        public required string ContentType { get; set; }
        public DateTime CreatedDate { get; set; }
        public required string Author { get; set; }
        public int PageCount { get; set; }
    }