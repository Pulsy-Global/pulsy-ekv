using Pulsy.EKV.Node;

Pulsy.SlateDB.SlateDb.InitLogging(Pulsy.SlateDB.Options.LogLevel.Warn);

EkvApp.Build(args).Run();
