using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Reflection;

public class Program
{
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseStartup<Startup>();
            });
}

public class Startup
{
    public IConfiguration Configuration { get; }

    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddControllers();

        services.AddDbContext<AppDbContext>(options =>
            options.UseSqlite(Configuration.GetConnectionString("SqliteConnection")));

        services.AddSingleton<IElasticClient>(sp =>
        {
            var settings = new ConnectionSettings(new Uri(Configuration["ElasticsearchUri"]))
                .DefaultIndex(Configuration["ElasticsearchIndex"]);
            return new ElasticClient(settings);
        });

        services.AddSingleton<ElasticsearchSyncService>();
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseRouting();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();
        });

        using (var serviceScope = app.ApplicationServices.GetRequiredService<IServiceScopeFactory>().CreateScope())
        {
            var dbContext = serviceScope.ServiceProvider.GetService<AppDbContext>();
            dbContext.Database.EnsureCreated();
        }
    }
}

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<ElasticsearchDocument> ElasticsearchDocuments { get; set; }
    public DbSet<SyncHistoryLog> SyncHistoryLogs { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {

        // Define DB structure to store the document
        modelBuilder.Entity<ElasticsearchDocument>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.IdElasticsearch).HasMaxLength(50);
            entity.Property(e => e.IntegrationErrorMessage).HasMaxLength(5000).IsRequired(false);
            entity.Property(e => e.ServiceProviderCode).HasMaxLength(100).IsRequired(false);
            entity.Property(e => e.Reference).HasMaxLength(13).IsRequired(false);
            entity.Property(e => e.IncomingModel).HasMaxLength(5000).IsRequired(false);
            entity.Property(e => e.OutgoingModel).HasMaxLength(5000).IsRequired(false);
            entity.Property(e => e.Url).HasMaxLength(50).IsRequired(false);
            entity.Property(e => e.RequestId).HasMaxLength(36).IsRequired(false);
            entity.Property(e => e.ModelType).HasMaxLength(4).IsRequired(false);
            entity.Property(e => e.PartnerCode).HasMaxLength(50).IsRequired(false);
            entity.Property(e => e.CarrierCode).HasMaxLength(4).IsRequired(false);
            entity.Property(e => e.IEProcess).HasMaxLength(5000).IsRequired(false);
        });

        modelBuilder.Entity<SyncHistoryLog>()
            .HasKey(e => e.Id);
    }
}

public class ElasticsearchSyncService
{
    private readonly ILogger<ElasticsearchSyncService> _logger;
    private readonly IElasticClient _elasticClient;
    private readonly IServiceScopeFactory _serviceScopeFactory;

    public ElasticsearchSyncService(
        ILogger<ElasticsearchSyncService> logger,
        IElasticClient elasticClient,
        IServiceScopeFactory serviceScopeFactory)
    {
        _logger = logger;
        _elasticClient = elasticClient;
        _serviceScopeFactory = serviceScopeFactory;
    }

    public async Task PerformSync(DateTime startDate)
    {
        var syncStartTime = DateTime.UtcNow;
        int documentCount = 0;

        try
        {
            var documents = await SearchElasticsearch(startDate);
            documentCount = documents.Count;
            
            await LoadDataToSqlite(documents);

            _logger.LogInformation($"Successfully processed {documentCount} documents.");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error occurred: {ex.Message}");
        }
        finally
        {
            var syncEndTime = DateTime.UtcNow;
            await LogSyncHistory(syncStartTime, syncEndTime, documentCount);
        }
    }

    private async Task<List<ElasticsearchDocument>> SearchElasticsearch(DateTime startDate)
{
    try
    {
        // Possible to log in Nest 7.x
        var searchDescriptor = new SearchDescriptor<object>()
            .Query(q => q
                .DateRange(r => r
                    .Field("date")
                    .GreaterThanOrEquals(startDate)
                    .LessThan("now")
                    .Format("strict_date_optional_time")
                )
            )
            .Size(10000)
            .Sort(sort => sort.Ascending("date"));

        var searchResponse = await _elasticClient.SearchAsync<object>(searchDescriptor);

        _logger.LogInformation($"Total hits: {searchResponse.Total}");
        
        if (!searchResponse.IsValid)
        {
            _logger.LogError($"Elasticsearch error: {searchResponse.ServerError?.Error}");
            return new List<ElasticsearchDocument>();
        }

        var documents = new List<ElasticsearchDocument>();
        foreach (var hit in searchResponse.Hits)
        {
            try
            {
                _logger.LogDebug($"Processing hit with ID: {hit.Id}");
                
                var sourceJson = JsonConvert.SerializeObject(hit.Source);
                _logger.LogDebug($"Source JSON: {sourceJson}");
                
                var source = JObject.Parse(sourceJson);
                
                // Add the id from hit to the source object
                source["id"] = hit.Id;
                
                var document = new ElasticsearchDocument();
                document.MapProperties(source);
                documents.Add(document);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing hit: {ex.Message}");
            }
        }

        if (documents.Any())
        {
            _logger.LogInformation($"First document: {JsonConvert.SerializeObject(documents.First())}");
        }
        else
        {
            _logger.LogWarning("No documents were successfully processed.");
        }

        return documents;
    }
    catch (Exception ex)
    {
        _logger.LogError($"Error in SearchElasticsearch: {ex.Message}");
        _logger.LogError($"Stack trace: {ex.StackTrace}");
        throw;
    }
}

    private ElasticsearchDocument MapToElasticsearchDocument(JObject source, Guid id)
    {
        var doc = source.ToObject<ElasticsearchDocument>();
        doc.IdElasticsearch = id.ToString();
        return doc;
    }

    private async Task LoadDataToSqlite(List<ElasticsearchDocument> documents)
    {
        using var scope = _serviceScopeFactory.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        foreach (var doc in documents)
        {
            var existingDoc = await dbContext.ElasticsearchDocuments
                .FirstOrDefaultAsync(d => d.IdElasticsearch == doc.IdElasticsearch);

            if (existingDoc != null)
            {
                dbContext.Entry(existingDoc).CurrentValues.SetValues(doc);
            }
            else
            {
                dbContext.ElasticsearchDocuments.Add(doc);
            }
        }

        await dbContext.SaveChangesAsync();
    }

    private async Task LogSyncHistory(DateTime startTime, DateTime endTime, int documentCount)
    {
        // Create a log of a performed sync
        using var scope = _serviceScopeFactory.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var syncHistoryLog = new SyncHistoryLog
        {
            SyncStartDatetime = startTime,
            SyncEndDatetime = endTime,
            CopiedDocumentCount = documentCount
        };

        dbContext.SyncHistoryLogs.Add(syncHistoryLog);
        await dbContext.SaveChangesAsync();
    }
}

public class ElasticsearchDocument
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    [JsonProperty("id")]
    public string? IdElasticsearch { get; set; }

    [JsonProperty("integrationStatus")]
    public int? IntegrationStatus { get; set; }

    [JsonProperty("date")]
    public DateTime Date { get; set; }

    [JsonProperty("updateDate")]
    public DateTime UpdateDate { get; set; }

    [JsonProperty("integrationErrorMessage")]
    public string? IntegrationErrorMessage { get; set; }

    [JsonProperty("organizationId")]
    public int? OrganizationId { get; set; }

    [JsonProperty("serviceProviderCode")]
    public string? ServiceProviderCode { get; set; }

    [JsonProperty("mappingId")]
    public int? MappingId { get; set; }

    [JsonProperty("reference")]
    public string? Reference { get; set; }

    [JsonProperty("incomingModel")]
    public string? IncomingModel { get; set; }

    [JsonProperty("outgoingModel")]
    public string? OutgoingModel { get; set; }

    [JsonProperty("url")]
    public string? Url { get; set; }

    [JsonProperty("messageType")]
    public int? MessageType { get; set; }

    [JsonProperty("retriggered")]
    public int? Retriggered { get; set; }

    [JsonProperty("requestId")]
    public string? RequestId { get; set; }

    [JsonProperty("source")]
    public int? Source { get; set; }

    [JsonProperty("contentType")]
    public int? ContentType { get; set; }

    [JsonProperty("version")]
    public int? Version { get; set; }

    [JsonProperty("modelType")]
    public string? ModelType { get; set; }

    [JsonProperty("partnerCode")]
    public string? PartnerCode { get; set; }

    [JsonProperty("carrierCode")]
    public string? CarrierCode { get; set; }

    [JsonProperty("iEProcess")]
    public string? IEProcess { get; set; }

    public void MapProperties(JObject source)
    {
        foreach (var property in GetType().GetProperties())
        {
            var jsonPropertyAttribute = property.GetCustomAttributes(typeof(JsonPropertyAttribute), false)
                                               .FirstOrDefault() as JsonPropertyAttribute;
            
            if (jsonPropertyAttribute != null && source.TryGetValue(jsonPropertyAttribute.PropertyName, out var value))
            {
                if (property.PropertyType == typeof(int?) && value.Type == JTokenType.String)
                {
                    if (int.TryParse(value.ToString(), out int intValue))
                    {
                        property.SetValue(this, intValue);
                    }
                    else
                    {
                        property.SetValue(this, null);
                    }
                }
                else
                {
                    property.SetValue(this, value.ToObject(property.PropertyType));
                }
            }
        }
    }

}

public class SyncHistoryLog
{
    public int Id { get; set; }
    public DateTime SyncStartDatetime { get; set; }
    public DateTime SyncEndDatetime { get; set; }
    public int CopiedDocumentCount { get; set; }
}

[ApiController]
[Route("api")]
public class SyncController : ControllerBase
{
    private readonly ElasticsearchSyncService _syncService;
    private readonly AppDbContext _dbContext;

    public SyncController(ElasticsearchSyncService syncService, AppDbContext dbContext)
    {
        _syncService = syncService;
        _dbContext = dbContext;
    }

    [HttpPost("sync")]
    public async Task<IActionResult> StartSync([FromQuery] DateTime startDate)
    {
        await _syncService.PerformSync(startDate);
        return Ok("Sync started successfully");
    }

    [HttpGet("log")]
    public async Task<IActionResult> GetSyncLog([FromQuery] int page = 1, [FromQuery] int pageSize = 10)
    {
        var syncHistory = await _dbContext.SyncHistoryLogs
            .OrderByDescending(s => s.SyncStartDatetime)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();

        return Ok(syncHistory);
    }

    [HttpGet("data")]
    public async Task<IActionResult> GetSyncedData([FromQuery] int page = 1, [FromQuery] int pageSize = 10)
    {
        var syncedData = await _dbContext.ElasticsearchDocuments
            .OrderByDescending(d => d.Date)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();

        return Ok(syncedData);
    }
}