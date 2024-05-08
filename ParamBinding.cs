using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.WebJobs.Host.Config;
using System;
using System.Collections.Generic;
using System.Text;

namespace ParamBinding
{
    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.ReturnValue)]
    [Binding]
    public sealed class ParamAttribute : Attribute
    {
        [AutoResolve(Default = "")]
        public string Param { get; set; }

        public ParamAttribute(string param)
        {
            Param = param;
        }

    }

    [Extension("ParamExtension")]
    public class ParamExtension : IExtensionConfigProvider
    {
        /// <summary>
        /// This callback is invoked by the WebJobs framework before the host starts execution. 
        /// It should add the binding rules and converters for our new <see cref="SampleAttribute"/> 
        public void Initialize(ExtensionConfigContext context)
        {
            // Create an input rules for the Sample attribute.
            var rule = context.AddBindingRule<ParamAttribute>();

            rule.BindToInput<string>(BuildFromAttribute);
        }

        private string BuildFromAttribute(ParamAttribute paramAttribute)
        {
            return paramAttribute.Param;
        }
    }


}
