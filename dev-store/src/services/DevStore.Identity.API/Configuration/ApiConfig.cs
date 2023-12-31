using DevStore.Core.Http;
using DevStore.Core.Messages.Integration;
using DevStore.WebAPI.Core.Configuration;
using DevStore.WebAPI.Core.User;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace DevStore.Identity.API.Configuration
{
    public static class ApiConfig
    {
        public static IServiceCollection AddApiConfiguration(this IServiceCollection services)
        {
            services.AddSingleton<IRestClient, RestClient>();

            services.AddHttpClient(nameof(UserRegisteredIntegrationEvent), options =>
            {
                options.BaseAddress = new System.Uri("https://localhost:5441/customers/create");
            });

            services.AddControllers();

            services.AddScoped<IAspNetUser, AspNetUser>();

            services.AddGenericHealthCheck();

            return services;
        }

        public static IApplicationBuilder UseApiConfiguration(this WebApplication app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthConfiguration();

            app.UseJwksDiscovery();

            app.UseGenericHealthCheck("/healthz");

            return app;
        }
    }
}