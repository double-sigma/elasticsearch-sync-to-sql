FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["elasticsearch_sync.csproj", "./"]
RUN dotnet restore "elasticsearch_sync.csproj"
COPY . .
RUN dotnet build "elasticsearch_sync.csproj" -c Release -o /app/build

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/build .
ENTRYPOINT ["dotnet", "elasticsearch_sync.dll"]