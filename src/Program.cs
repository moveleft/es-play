namespace Monsenso
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Nest;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    public class Program
    {
        public static void Main()
        {
            var uri = new Uri("http://log02.corp.monsenso.com:9200");
            var settings = new ConnectionSettings(uri);
            settings.DisableDirectStreaming();
            var client = new ElasticClient(settings);

            var passwords = 0;
            var authHeaders = 0;

            foreach(var idx in client.CatIndices().Records)
            {
                var idxName = idx.Index;
                // var idxName = "logstash-2018.02.09";
                System.Console.WriteLine(idxName);
                var results =
                    client.Search<dynamic>(s => s
                        .Index(idxName)
                        .Size(2000)
                        .Type("logevent")
                        .Query(q =>
                            (q.Exists(e => e.Field("fields.RequestForm.password")) &&
                             !q.Match(m => m.Field("fields.RequestForm.password").Query("*** Redacted ***"))
                            )
                            ||
                            (q.Exists(e => e.Field("fields.RequestHeaders.Authorization")) &&
                             !q.Match(m => m.Field("fields.RequestHeaders.Authorization").Query("*** Redacted ***"))
                            )
                        )
                    );

                // System.Console.WriteLine(results.DebugInformation);
                // return;

                System.Console.WriteLine(results.Total);

                foreach(var hit in results.Hits)
                {
                    var path = (DocumentPath<dynamic>)hit.Id;
                    path.Type("logevent");
                    var doc = client.Get<dynamic>(path, x => x.Index(idxName));
                    if (doc.Source == null)
                    {
                        System.Console.WriteLine("null!!!");
                        continue;
                    }

                    // var jsonString = JsonConvert.SerializeObject(
                    //     doc,
                    //     new JsonSerializerSettings
                    //     {
                    //         Converters = new List<JsonConverter> { new StringEnumConverter() },
                    //         ContractResolver = new ElasticContractResolver(settings, new List<Func<Type, JsonConverter>>())
                    //     });
                    // System.Console.WriteLine(jsonString);

                    bool update = false;
                    if (doc.Source.fields.RequestForm?.password != null &&
                        doc.Source.fields.RequestForm?.password != "*** Redacted ***")
                    {
                        update = true;
                        passwords++;
                        doc.Source.fields.RequestForm.password = "*** Redacted ***";
                    }

                    if (doc.Source.fields.RequestHeaders?.Authorization != null &&
                        doc.Source.fields.RequestHeaders?.Authorization != "*** Redacted ***")
                    {
                        update = true;
                        authHeaders++;
                        doc.Source.fields.RequestHeaders.Authorization = "*** Redacted ***";
                    }

                    if (update)
                    {
                        System.Console.WriteLine(hit.Id);
                        client.Update<dynamic>(path, u => u.Doc(doc.Source).Index(idxName));
                    }
                }
            }

            System.Console.WriteLine("Passwords removed: " + passwords);
            System.Console.WriteLine("Authorization headers removed: " + authHeaders);
        }
    }
}
