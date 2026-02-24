FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080

FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Directory.Build.props", "./"]
COPY ["Directory.Packages.props", "./"]
COPY ["global.json", "./"]
COPY ["Pulsy.EKV.Node/Pulsy.EKV.Node.csproj", "Pulsy.EKV.Node/"]
COPY ["proto/ekv.proto", "proto/"]
RUN dotnet restore "Pulsy.EKV.Node/Pulsy.EKV.Node.csproj"
COPY . .
WORKDIR "/src/Pulsy.EKV.Node"
RUN dotnet build "./Pulsy.EKV.Node.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Pulsy.EKV.Node.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Pulsy.EKV.Node.dll"]
