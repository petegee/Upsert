using Eventuous;
using Eventuous.AspNetCore;
using Eventuous.AspNetCore.Web;
using Eventuous.EventStore;
using Eventuous.EventStore.Subscriptions;
using Eventuous.Projections.MongoDB;
using Eventuous.Projections.MongoDB.Tools;
using Eventuous.Subscriptions.Context;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Driver;
using Serilog;

TypeMap.RegisterKnownEventTypes(typeof(Upserted).Assembly);
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Verbose().Enrich.FromLogContext().WriteTo.Console()
    .CreateLogger();

var builder = WebApplication.CreateBuilder(args);
builder.Host.UseSerilog();
builder.Services
    .AddEndpointsApiExplorer()
    .AddEventStoreClient("esdb://localhost:2113?tls=false")
    .AddSingleton(Mongo.ConfigureMongo())
    .AddAggregateStore<EsdbEventStore>()
    .AddCheckpointStore<MongoCheckpointStore>()
    .AddCommandService<MyCommandService, MyAggregate>()
    .AddSubscription<StreamSubscription, StreamSubscriptionOptions>("MySubscription", subBuilder => subBuilder
            .Configure(cfg => {
                    cfg.StreamName = new StreamName("$ce-MyAggregate");
                    cfg.ResolveLinkTos = true;
                    cfg.ThrowOnError = true;
                }
            )
            .UseCheckpointStore<MongoCheckpointStore>()
            .AddEventHandler<MyProjection>())
    .AddSwaggerGen()
    .AddControllers();

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();
app.UseRouting();
app.MapControllers();
app.AddEventuousLogs();
app.Run();


[ApiController]
[Route("[controller]")]
public class MyController : CommandHttpApiBase<MyAggregate>
{
    public MyController(ICommandService<MyAggregate> service, MessageMap? commandMap = null) : base(service, commandMap) { }
    
    [HttpPost]
    [Route("Upsert")]
    public Task<ActionResult<Result>> Upsert(
        [FromBody] UpsertIt cmd,
        CancellationToken cancellationToken
    ) => Handle(cmd, cancellationToken);
}

public class MyCommandService: CommandService<MyAggregate, MyAggregateState, MyAggregateId> {
    public MyCommandService(IAggregateStore store) : base(store) {
        OnNew<UpsertIt>(
            cmd => new MyAggregateId(cmd.Name),
            (agg, cmd) => agg.Upsert(cmd.Name, "insert", DateTimeOffset.Now));

        // OnExisting<UpsertIt>(
        //     cmd => new MyAggregateId(cmd.Name),
        //     (agg, cmd) => agg.Upsert(cmd.Name, "updated", DateTimeOffset.Now));
    }
}

public class MyProjection : MongoProjector<MyReadModel> {
    public MyProjection(IMongoDatabase database) : base(database) {
        On<Upserted>(stream => stream.GetId(), HandleUpserted);
    }

    private UpdateDefinition<MyReadModel> HandleUpserted(IMessageConsumeContext<Upserted> ctx, UpdateDefinitionBuilder<MyReadModel> update)
        => update.SetOnInsert(x => x.Id, ctx.Stream.GetId())
            .Set(x => x.Action, ctx.Message.Action)
            .Set(x => x.Timestamp, ctx.Message.Timestamp);
}

public record UpsertIt(string Name);
[EventType("Upserted")]
public record Upserted(string Name, string Action, DateTimeOffset Timestamp);
public record MyAggregateId(string Value) : AggregateId(Value);
public class MyAggregate : Aggregate<MyAggregateState> {
    public void Upsert(string name, string action, DateTimeOffset now) {
        Apply(new Upserted(name, action, now));
    }
}
public record MyAggregateState : State<MyAggregateState> {
    public string Name { get; init; }
    public string Action { get; init; }
    public DateTimeOffset Timestamp { get; init; }
}    
public record MyReadModel : ProjectedDocument {
    public MyReadModel(string Id) : base(Id) {}
    public string Name { get; init; }
    public string Action{ get; init; }
    public DateTimeOffset Timestamp{ get; init; }
}
public static class Mongo {
    public static IMongoDatabase ConfigureMongo() {
        var settings = MongoClientSettings.FromConnectionString("mongodb://localhost:27017");
        settings.Credential = new MongoCredential(null, new MongoInternalIdentity("admin", "mongoadmin"), new PasswordEvidence("secret"));
        return new MongoClient(settings).GetDatabase("Upsert");
    }
}