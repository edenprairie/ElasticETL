using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MSSQLElasticSearchETL
{
    public class ESConfiguration : ConfigurationSection
    {
        [ConfigurationProperty("elasticSearchConnectionString")]
        public string ElasticSearchConnectionString { get { return (string)base["elasticSearchConnectionString"]; } }

        [ConfigurationProperty("ES")]
        public ESElementCollection ES { get { return (ESElementCollection)base["ES"]; } }
    }

    public class ESElementCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
 	        return new ESElement();
        }

        public ESElement this[int index]
        {
            get { return (ESElement)base.BaseGet(index); }
            set
            {
                if (base.BaseGet(index) != null)
                {
                    base.BaseRemoveAt(index);
                }
                base.BaseAdd(index, value);
            }
        }

        //The key is the Table + Index combination. Never used, but inheritance requires it. 
        protected override object GetElementKey(ConfigurationElement element)
        {
            return (element as ESElement).DatabaseTable + "-" + (element as ESElement).ElasticIndex;
        }
    }

    public class ESElement : ConfigurationElement
    {
        [ConfigurationProperty("databaseTable", IsRequired=true)]
        public string DatabaseTable { get { return (string)base["databaseTable"]; } }

        [ConfigurationProperty("elasticIndex", IsRequired=true)]
        public string ElasticIndex { get { return (string)base["elasticIndex"]; } }

        [ConfigurationProperty("elasticType", IsRequired = true)]
        public string ElasticType { get { return (string)base["elasticType"]; } }
    }
}
