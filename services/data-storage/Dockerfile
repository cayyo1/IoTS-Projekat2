# --- Build stage ---
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY DataStorage.csproj .
RUN dotnet restore
COPY . .
RUN dotnet publish DataStorage.csproj -c Release -o /app

# --- Runtime stage ---
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENV DOTNET_ENVIRONMENT=Production
ENTRYPOINT ["dotnet", "DataStorage.dll"]