using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Data; 
using System.Text;
using System.Threading.Tasks;
using System.IO; 
using Nest;
using Elasticsearch.Net;
using System.Xml;
using System.Xml.Serialization;
using Nlx = Novologix.Shared.Payload.Library.Domain2.Schemas;
using NlxClass = Novologix.Shared.Payload.Library.Domain2.Classes;
using System.Xml.Serialization;
using Dapper; 

namespace MSSQLElasticSearchETL
{
    class Program
    {
        static void Main(string[] args)
        {

            ESConfiguration _esConfiguration = (ESConfiguration)ConfigurationManager.GetSection("ESConfiguration");

            ConnectionSettings searchSettings = new ConnectionSettings(new Uri(_esConfiguration.ElasticSearchConnectionString));
            var searchClient = new ElasticClient(searchSettings);

            #region "Debugging.."
            //while (true)
            //{
            //    //Connect to DB and listen for a message. 
            //    using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["queueDatabase"].ConnectionString))
            //    {
            //        sqlConnection.Open();

            //        SqlCommand receiveCommand = new SqlCommand("WAITFOR( RECEIVE TOP(1) CONVERT(XML, message_body) AS Message FROM esUpdateReceiveQueue )", sqlConnection);
            //        SqlCommand retrieveDataCommand;

            //        esMessage esMessage;
            //        using (SqlDataReader receiveCommandReader = receiveCommand.ExecuteReader())
            //        {
            //            //Deserialize message. 
            //            receiveCommandReader.Read();
            //            XmlSerializer serializer = new XmlSerializer(typeof(esMessage));
            //            esMessage = (esMessage)serializer.Deserialize(receiveCommandReader.GetXmlReader(0));

            //            //Get the entire record out of the DB. 
            //            //retrieveDataCommand = new SqlCommand(string.Format("SELECT TOP(1) * FROM {0} WHERE AuthId = {1}", esMessage.DatabaseTable, esMessage.Id), sqlConnection);
            //            retrieveDataCommand = new SqlCommand(string.Format("SELECT TOP(1) * FROM Nlxprocessing..ClaimTransaction", esMessage.DatabaseTable, esMessage.Id), sqlConnection);
            //        }

            //        using (SqlDataReader retrieveDataCommandReader = retrieveDataCommand.ExecuteReader())
            //        {
            //            JObject item = null;
            //            //Read it from the DB and store in a simple JSON object. 

            //            if (retrieveDataCommandReader.Read())
            //            {
            //                //item = new JObject();

            //                //for (int i = 0; i < retrieveDataCommandReader.FieldCount; i++)
            //                //{
            //                //    item[retrieveDataCommandReader.GetName(i)] = new JValue(retrieveDataCommandReader.GetValue(i));
            //                //}


            //                DataRow ClaimReqDataRow = retrieveDataCommandReader.Rows[0];

            //                Nlx.Claim clm = new Nlx.Claim();
            //                clm = (Nlx.Claim)serializer.Deserialize(new StringReader(retrieveDataCommandReader["NLXClaim"].ToString()));

            //            }

            //            //Foreach es that wants this type of record. Send it. 
            //            foreach (var esRequired in _esConfiguration.ess.Cast<esElement>().Where(x => x.DatabaseTable == esMessage.DatabaseTable))
            //            {
            //                if (item != null)
            //                    searchClient.Index(esRequired.ElasticIndex, esRequired.ElasticType, esMessage.Id.ToString(), item.ToString());
            //                else
            //                    searchClient.Delete(esRequired.ElasticIndex, esRequired.ElasticType, esMessage.Id.ToString());
            //            }
            //        }
            //    }
            //}
            #endregion


            int maxClaimID = 1237167; 

            while (true)
           {
                using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["queueDatabase"].ConnectionString))
                {
                    string GetMessage = string.Format(@"SELECT TOP(100) nlxclaim,* FROM Nlxprocessing..ClaimTransaction where claimtransactionidentifier < {0} order by claimtransactionidentifier desc", maxClaimID.ToString());
                    SqlCommand cmdMessage = new SqlCommand(GetMessage, sqlConnection);
                    SqlCommand CmdOdsAuth = new SqlCommand();
                    CmdOdsAuth.Connection = sqlConnection;
                    sqlConnection.Open();

                    //string query = string.Format(@"SELECT TOP(1000) nlxclaim,* FROM Nlxprocessing..ClaimTransaction where claimtransactionidentifier < {0} order by claimtransactionidentifier desc", maxClaimID.ToString());

                    SqlDataReader mReader = cmdMessage.ExecuteReader();
                    DataTable tbl = new DataTable("ExternalActivationClaim");
                    if (mReader.HasRows)
                    {
                        tbl.Load(mReader);

                        mReader.Close();

                    }
                    var descriptor = new BulkDescriptor();
                    foreach (DataRow ClaimDataRow in tbl.Rows)
                    {
                        XmlSerializer serializer = new XmlSerializer(typeof(Nlx.Claim));

                        NlxClass.MedRxEncryptionKey key = new NlxClass.MedRxEncryptionKey("LKW34ippHJ34Cvvw+OkkRE56GdvWRETT", "JeLpo301/KJ=");


                        Nlx.Claim clm = new Nlx.Claim();

                        clm = (Nlx.Claim)serializer.Deserialize(new StringReader(ClaimDataRow["NLXClaim"].ToString()));
                        clm.EncryptionKey = key;


                        #region "Generating Json"
                        //clm.Subscriber.EncryptionKey = key;
                        //FileStream fs = File.Open(@"c:\temp\auth.xml", FileMode.OpenOrCreate);
                        //System.Xml.Serialization.XmlSerializer s = new System.Xml.Serialization.XmlSerializer(clm.GetType());
                        //s.Serialize(fs, clm);
                        //MemoryStream stream1 = new MemoryStream();
                        //System.Runtime.Serialization.Json.DataContractJsonSerializer ser = new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(Nlx.Claim));
                        //ser.WriteObject(stream1, clm);
                        //stream1.Position = 0;
                        //StreamReader sr = new StreamReader(stream1);
                        //Console.Write("JSON form of claim object: ");
                        //Console.WriteLine(sr.ReadToEnd());

                        //string output = JsonConvert.SerializeObject(clm);
                        #endregion

                        int claimID = System.Convert.ToInt32(ClaimDataRow["ClaimTransactionIdentifier"]);
                        //foreach (var esRequired in _esConfiguration.ess.Cast<esElement>().Where(x => x.DatabaseTable == "claim"))
                        //{
                        //    if (clm != null)
                        //        searchClient.Index(esRequired.ElasticIndex, esRequired.ElasticType, claimID, sr.ReadToEnd());
                        //    else
                        //        searchClient.Delete(esRequired.ElasticIndex, esRequired.ElasticType, claimID);
                        //}

                        //string output = JsonConvert.SerializeObject(clm);
                        descriptor.Index<Nlx.Claim>(i => i
                                            .Index("testclaims")
                                            .Type("claim")
                                            .Id(claimID)
                                            .Document(clm));

                       maxClaimID = claimID;
                    }
                    searchClient.Bulk(descriptor);
                }
                
            }
            if (false)
            {
                using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["queueDatabase"].ConnectionString))
                {
                    string GetMessage = @"SELECT TOP(10) nlxauth,* FROM Nlxprocessing..authTransaction WHERE planid = 1000000183 order by authtransactionidentifier desc";
                    SqlCommand cmdMessage = new SqlCommand(GetMessage, sqlConnection);
                    SqlCommand CmdOdsAuth = new SqlCommand();
                    CmdOdsAuth.Connection = sqlConnection;
                    sqlConnection.Open();

                    SqlDataReader mReader = cmdMessage.ExecuteReader();
                    DataTable tbl = new DataTable("ExternalActivationAuth");
                    if (mReader.HasRows)
                    {
                        tbl.Load(mReader);

                        //while (mReader.Read())
                        //{
                        //    tbl.Load(mReader);

                        //}
                        mReader.Close();

                    }
                    var descriptor = new BulkDescriptor();
                    foreach (DataRow AuthDataRow in tbl.Rows)
                    {
                        XmlSerializer serializer = new XmlSerializer(typeof(Nlx.AuthorizationRequest));

                        NlxClass.MedRxEncryptionKey key = new NlxClass.MedRxEncryptionKey("LKW34ippHJ34Cvvw+OkkRE56GdvWRETT", "JeLpo301/KJ=");


                        Nlx.AuthorizationRequest auth = new Nlx.AuthorizationRequest();


                        auth = (Nlx.AuthorizationRequest)serializer.Deserialize(new StringReader(AuthDataRow["NLXAuth"].ToString()));
                        auth.EncryptionKey = key;
                        //auth.Subscriber.EncryptionKey = key;
                        //auth.Subscriber.Member.Memberships[0].Member.EncryptionKey = key;
                        //auth.EligibilitySources[0].EncryptionKey = key;
                        //auth.EligibilitySources[0].Memberships[0].Member.EncryptionKey = key;
                        //auth.EligibilitySources[0].Memberships[0].MembershipDates[0].BenefitPlanGroup.BenefitPlan.BenefitPlanGroups[0].MembershipDates[0].Membership.Member.EncryptionKey = key;
                        //auth.Patient.MembershipDates[0].BenefitPlanGroup.BenefitPlan.BenefitPlanGroups[0].MembershipDates[0].Membership.Member.EncryptionKey = key; 
                        //FileStream fs = File.Open(@"c:\temp\auth1.xml", FileMode.OpenOrCreate);
                        //System.Xml.Serialization.XmlSerializer s = new System.Xml.Serialization.XmlSerializer(auth.GetType());
                        //s.Serialize(fs, auth);
                        //System.Xml.Serialization.XmlSerializer s = new System.Xml.Serialization.XmlSerializer(auth.GetType());
                        //s.Serialize(Console.Out, auth);
                        //string output = JsonConvert.SerializeObject(auth);

                        string authID = AuthDataRow["AuthTransactionIdentifier"].ToString();

                        //foreach (var esRequired in _esConfiguration.ess.Cast<esElement>().Where(x => x.DatabaseTable == "auth"))
                        //{
                        //    if (auth != null)
                        //        searchClient.Index(esRequired.ElasticIndex, esRequired.ElasticType, authID, output);
                        //    else
                        //        searchClient.Delete(esRequired.ElasticIndex, esRequired.ElasticType, authID);
                        //}

                        //descriptor.Index<Doc>(op => op.Document(new Doc{ }).Id(authID));
                        //descriptor.Index<object>(i => i
                        //                    .Index("someindex")
                        //                    .Type("SomeType")
                        //                    .Id("someid")
                        //                    .Document(auth));

                    }
                    
                }
            }
        }
    }
}
