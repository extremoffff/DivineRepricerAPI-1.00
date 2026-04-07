using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using SteamKit2;
using SteamKit2.Authentication;
using SteamKit2.Internal;

const string UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";

var port = GetIntEnv("STEAM_BRIDGE_PORT", 8787);

var sessions = new ConcurrentDictionary<string, CachedSession>(StringComparer.Ordinal);
var profileLocks = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal);
var sessionStorePath = Path.Combine(AppContext.BaseDirectory, "steam_sessions.json");
var sessionStoreLock = new object();
var restoredSessionsNeedingCheck = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(port);
});
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = null;
    options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
    options.SerializerOptions.WriteIndented = false;
});

var app = builder.Build();

app.MapGet("/health", () => Results.Json(new HealthResponse
{
    Ok = true,
    Sessions = sessions.Count,
}));

app.MapPost("/access-token", async (AccessTokenRequest request) =>
{
    var profileName = GetProfileName(request);

    Log("POST /access-token:start", new
    {
        profile_name = profileName,
        force_new_session = request.ForceNewSession,
        body = MaskProfile(request),
    });

    try
    {
        var session = await EnsureSessionAsync(request, request.ForceNewSession).ConfigureAwait(false);
        session = await EnsureAccessTokenReadyAsync(request, session).ConfigureAwait(false);

        var response = new AccessTokenResponse
        {
            Success = true,
            ProfileName = profileName,
            SteamId64 = session.SteamId64,
            AccessToken = session.MarketWebApiToken,
            TokenSource = session.MarketWebApiTokenSource,
        };

        Log("POST /access-token:ok", new
        {
            profile_name = profileName,
            success = true,
            token_len = response.AccessToken?.Length ?? 0,
            token_source = response.TokenSource,
        });

        return Results.Json(response);
    }
    catch (Exception ex)
    {
        Log("POST /access-token:error", new
        {
            profile_name = profileName,
            error = ex.ToString(),
        });

        return Results.Json(new ErrorResponse
        {
            Success = false,
            Error = ex.Message,
        }, statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/steam/inventory", async (SteamInventoryRequest request) =>
{
    var profileName = GetProfileName(request);

    Log("POST /steam/inventory:start", new
    {
        profile_name = profileName,
        force_new_session = request.ForceNewSession,
        body = MaskProfile(request),
        count = request.Count,
    });

    try
    {
        var session = await EnsureSessionAsync(request, request.ForceNewSession).ConfigureAwait(false);
        var count = request.Count <= 0 ? 2000 : Math.Min(request.Count, 5000);
        var items = await FetchFullSteamInventoryAsync(session.Proxy, session.SteamId64, session.SteamWebCookie, count).ConfigureAwait(false);

        var response = new SteamInventoryResponse
        {
            Success = true,
            ProfileName = profileName,
            SteamId64 = session.SteamId64,
            Count = items.Count,
            Items = items,
        };

        Log("POST /steam/inventory:ok", new
        {
            profile_name = profileName,
            count = response.Count,
        });

        return Results.Json(response);
    }
    catch (Exception ex)
    {
        Log("POST /steam/inventory:error", new
        {
            profile_name = profileName,
            error = ex.ToString(),
        });

        return Results.Json(new ErrorResponse
        {
            Success = false,
            Error = ex.Message,
        }, statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/tradeoffer/send", async (TradeOfferSendRequest request) =>
{
    var profileName = GetProfileName(request);

    Log("POST /tradeoffer/send:start", new
    {
        profile_name = profileName,
        force_new_session = request.ForceNewSession,
        has_offer = request.Offer.HasValue && request.Offer.Value.ValueKind != JsonValueKind.Null && request.Offer.Value.ValueKind != JsonValueKind.Undefined,
    });

    try
    {
        var session = await EnsureSessionAsync(request, request.ForceNewSession).ConfigureAwait(false);
        if (!request.Offer.HasValue || request.Offer.Value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            throw new InvalidOperationException("offer is required");
        }

        var result = await SendTradeOfferAsync(session, request.Offer.Value).ConfigureAwait(false);

        return Results.Json(new TradeOfferSendResponse
        {
            Success = true,
            ProfileName = profileName,
            TradeOfferId = result.TradeOfferId,
            NeedsMobileConfirmation = result.NeedsMobileConfirmation,
            NeedsEmailConfirmation = result.NeedsEmailConfirmation,
        });
    }
    catch (Exception ex)
    {
        Log("POST /tradeoffer/send:error", new
        {
            profile_name = profileName,
            error = ex.ToString(),
        });

        return Results.Json(new ErrorResponse
        {
            Success = false,
            Error = ex.Message,
        }, statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/tradeoffer/confirm", async (TradeOfferActionRequest request) =>
{
    var profileName = GetProfileName(request);

    try
    {
        var session = await EnsureSessionAsync(request, request.ForceNewSession).ConfigureAwait(false);
        var tradeOfferId = RequireTradeOfferId(request.TradeOfferId);
        var ok = await ConfirmTradeOfferAsync(session, tradeOfferId).ConfigureAwait(false);

        return Results.Json(new TradeOfferActionResponse
        {
            Success = ok,
            ProfileName = profileName,
            TradeOfferId = tradeOfferId,
            Action = "confirm",
            Error = ok ? null : "confirmation_not_found_or_failed",
        }, statusCode: ok ? StatusCodes.Status200OK : StatusCodes.Status409Conflict);
    }
    catch (Exception ex)
    {
        Log("POST /tradeoffer/confirm:error", new
        {
            profile_name = profileName,
            error = ex.ToString(),
        });

        return Results.Json(new ErrorResponse
        {
            Success = false,
            Error = ex.Message,
        }, statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/tradeoffer/cancel", async (TradeOfferActionRequest request) =>
{
    var profileName = GetProfileName(request);

    try
    {
        var session = await EnsureSessionAsync(request, request.ForceNewSession).ConfigureAwait(false);
        var tradeOfferId = RequireTradeOfferId(request.TradeOfferId);
        await CancelTradeOfferAsync(session, tradeOfferId).ConfigureAwait(false);

        return Results.Json(new TradeOfferActionResponse
        {
            Success = true,
            ProfileName = profileName,
            TradeOfferId = tradeOfferId,
            Action = "cancel",
        });
    }
    catch (Exception ex)
    {
        Log("POST /tradeoffer/cancel:error", new
        {
            profile_name = profileName,
            error = ex.ToString(),
        });

        return Results.Json(new ErrorResponse
        {
            Success = false,
            Error = ex.Message,
        }, statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/tradeoffer/status", async (TradeOfferActionRequest request) =>
{
    var profileName = GetProfileName(request);

    try
    {
        var session = await EnsureSessionAsync(request, request.ForceNewSession).ConfigureAwait(false);
        var tradeOfferId = RequireTradeOfferId(request.TradeOfferId);
        var status = await GetTradeOfferStatusAsync(session, tradeOfferId).ConfigureAwait(false);

        return Results.Json(new TradeOfferStatusResponse
        {
            Success = true,
            ProfileName = profileName,
            TradeOfferId = tradeOfferId,
            TradeOfferState = status.TradeOfferState,
            ConfirmationMethod = status.ConfirmationMethod,
            TimeCreated = status.TimeCreated,
            TimeUpdated = status.TimeUpdated,
            ExpirationTime = status.ExpirationTime,
        });
    }
    catch (Exception ex)
    {
        Log("POST /tradeoffer/status:error", new
        {
            profile_name = profileName,
            error = ex.ToString(),
        });

        return Results.Json(new ErrorResponse
        {
            Success = false,
            Error = ex.Message,
        }, statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/session/invalidate", (InvalidateRequest request) =>
{
    var profileName = string.IsNullOrWhiteSpace(request.ProfileName)
        ? request.Name?.Trim() ?? string.Empty
        : request.ProfileName.Trim();

    if (string.IsNullOrWhiteSpace(profileName))
    {
        return Results.Json(new ErrorResponse
        {
            Success = false,
            Error = "profile_name is required",
        }, statusCode: StatusCodes.Status400BadRequest);
    }

    DestroySession(profileName, "invalidate_endpoint");

    return Results.Json(new InvalidateResponse
    {
        Success = true,
        ProfileName = profileName,
    });
});

LoadSessionsFromDisk();

AppDomain.CurrentDomain.ProcessExit += (_, __) =>
{
    try
    {
        SaveSessionsToDisk();
    }
    catch
    {
    }
};

Console.CancelKeyPress += (_, __) =>
{
    try
    {
        SaveSessionsToDisk();
    }
    catch
    {
    }
};

Log("listening", new
{
    url = $"http://127.0.0.1:{port}",
});

await app.RunAsync().ConfigureAwait(false);

async Task<CachedSession> EnsureSessionAsync(AccessTokenRequest request, bool forceNewSession)
{
    var profileName = GetProfileName(request);
    var wantedProxy = RequireProxy(request.Proxy);
    var wantedFingerprint = BuildSessionFingerprint(request, wantedProxy);

    var gate = profileLocks.GetOrAdd(profileName, _ => new SemaphoreSlim(1, 1));
    await gate.WaitAsync().ConfigureAwait(false);

    try
    {
        if (sessions.TryGetValue(profileName, out var cached))
        {
            var sameProxy = string.Equals(cached.Proxy, wantedProxy, StringComparison.Ordinal);
            var sameFingerprint = string.Equals(cached.Fingerprint, wantedFingerprint, StringComparison.Ordinal);

            // пока профиль тот же и force_new_session не запросили — всегда reuse
            if (!forceNewSession && sameProxy && sameFingerprint)
            {
                // если сессия поднята с диска после рестарта — один раз проверяем,
                // что cookie ещё вообще живая
                if (restoredSessionsNeedingCheck.TryRemove(profileName, out _))
                {
                    try
                    {
                        cached = await EnsureRestoredSessionReadyAsync(request, cached).ConfigureAwait(false);

                        Log("ensureSession:restored_ok", new
                        {
                            profile_name = profileName,
                        });
                    }
                    catch (Exception ex)
                    {
                        Log("ensureSession:restored_invalid", new
                        {
                            profile_name = profileName,
                            error = ex.Message,
                        });

                        DestroySession(profileName, "restored_invalid");
                        cached = null!;
                    }
                }

                if (sessions.TryGetValue(profileName, out cached))
                {
                    Log("ensureSession:reuse", new
                    {
                        profile_name = profileName,
                    });

                    return cached;
                }
            }
            else
            {
                DestroySession(
                    profileName,
                    forceNewSession ? "force_new_session" : "profile_changed"
                );
            }
        }

        Log("ensureSession:create_new", new
        {
            profile_name = profileName,
            force_new_session = forceNewSession,
        });

        var created = await CreateSessionAsync(request, wantedProxy, wantedFingerprint).ConfigureAwait(false);
        sessions[profileName] = created;
        SaveSessionsToDisk();
        return created;
    }
    finally
    {
        gate.Release();
    }
}

async Task<CachedSession> CreateSessionAsync(AccessTokenRequest request, string proxyUrl, string fingerprint)
{
    var profileName = GetProfileName(request);
    var username = (request.SteamUsername ?? string.Empty).Trim();
    var password = (request.SteamPassword ?? string.Empty).Trim();

    if (string.IsNullOrWhiteSpace(username))
    {
        throw new InvalidOperationException("steam_username is required");
    }

    if (string.IsNullOrWhiteSpace(password))
    {
        throw new InvalidOperationException("steam_password is required");
    }

    var ma = ResolveMaFile(request);

    Log("createSession:start", new
    {
        profile_name = profileName,
        has_proxy = true,
        has_username = !string.IsNullOrWhiteSpace(username),
        has_password = !string.IsNullOrWhiteSpace(password),
        has_shared_secret = !string.IsNullOrWhiteSpace(ma.SharedSecret),
        has_identity_secret = !string.IsNullOrWhiteSpace(ma.IdentitySecret),
        steamid64 = ma.SteamId64,
    });

    var proxyIp = await TestProxyOrThrowAsync(proxyUrl).ConfigureAwait(false);
    Log("createSession:proxy_ok", new
    {
        profile_name = profileName,
        proxy_ip = proxyIp,
    });

    var steamConfig = SteamConfiguration.Create(config =>
        config
            .WithProtocolTypes(ProtocolTypes.WebSocket)
            .WithHttpClientFactory(purpose =>
            {
                return CreateProxiedHttpClient(proxyUrl, TimeSpan.FromSeconds(30));
            })
    );

    var steamClient = new SteamClient(steamConfig);
    var manager = new CallbackManager(steamClient);

    var connectedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    var disconnectedTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    manager.Subscribe<SteamClient.ConnectedCallback>(_ =>
    {
        connectedTcs.TrySetResult(true);
    });

    manager.Subscribe<SteamClient.DisconnectedCallback>(cb =>
    {
        disconnectedTcs.TrySetResult($"Disconnected from Steam. UserInitiated={cb.UserInitiated}");
    });

    steamClient.Connect();

    using (var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
    {
        while (!connectCts.IsCancellationRequested)
        {
            manager.RunWaitCallbacks(TimeSpan.FromMilliseconds(250));

            if (connectedTcs.Task.IsCompleted)
                break;

            if (disconnectedTcs.Task.IsCompleted)
                throw new InvalidOperationException(await disconnectedTcs.Task.ConfigureAwait(false));
        }
    }

    if (!steamClient.IsConnected)
    {
        throw new InvalidOperationException("SteamClient failed to connect within timeout.");
    }

    var authSession = await steamClient.Authentication.BeginAuthSessionViaCredentialsAsync(new AuthSessionDetails
    {
        Username = username,
        Password = password,
        IsPersistentSession = false,
        PlatformType = EAuthTokenPlatformType.k_EAuthTokenPlatformType_MobileApp,
        ClientOSType = EOSType.Android9,
        Authenticator = new SharedSecretAuthenticator(ma.SharedSecret),
    }).ConfigureAwait(false);

    var pollResponse = await authSession.PollingWaitForResultAsync().ConfigureAwait(false);

    var refreshToken = (pollResponse.RefreshToken ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(refreshToken))
    {
        throw new InvalidOperationException("SteamKit returned empty refresh token");
    }

    var steamWebCookie = (pollResponse.AccessToken ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(steamWebCookie))
    {
        throw new InvalidOperationException("SteamKit returned empty web access token");
    }

    var steamId64 = ma.SteamId64;
    try
    {
        var steamIdFromSession = authSession.SteamID.ConvertToUInt64();
        if (steamIdFromSession != 0)
        {
            steamId64 = steamIdFromSession.ToString();
        }
    }
    catch
    {
        // fallback to mafile steamid64
    }

    try
    {
        Log("createSession:warmup_begin", new { profile_name = profileName });
        await WarmupInventoryAsync(proxyUrl, steamId64, steamWebCookie).ConfigureAwait(false);
        Log("createSession:warmup_ok", new { profile_name = profileName });
    }
    catch (Exception ex)
    {
        Log("createSession:warmup_error", new
        {
            profile_name = profileName,
            error = ex.Message,
        });
    }

    var marketWebApiToken = await FetchSteamCommunityWebApiTokenAsync(proxyUrl, steamId64, steamWebCookie).ConfigureAwait(false);
    var expiresAtUtc = TryReadJwtExpiry(steamWebCookie) ?? DateTimeOffset.UtcNow.AddHours(24);
    var marketTokenExpiresAtUtc = TryReadJwtExpiry(marketWebApiToken) ?? expiresAtUtc;
    var refreshTokenExpiresAtUtc = TryReadJwtExpiry(refreshToken) ?? DateTimeOffset.UtcNow.AddDays(180);

    var item = new CachedSession(
        ProfileName: profileName,
        Proxy: proxyUrl,
        Fingerprint: fingerprint,
        SteamId64: steamId64,
        SteamWebCookie: steamWebCookie,
        MarketWebApiToken: marketWebApiToken,
        RefreshToken: refreshToken,
        IdentitySecret: ma.IdentitySecret,
        DeviceId: string.IsNullOrWhiteSpace(ma.DeviceId) ? GenerateDeviceId(steamId64) : ma.DeviceId,
        CreatedAtUtc: DateTimeOffset.UtcNow,
        WebCookieExpiresAtUtc: expiresAtUtc,
        MarketWebApiTokenExpiresAtUtc: marketTokenExpiresAtUtc,
        RefreshTokenExpiresAtUtc: refreshTokenExpiresAtUtc,
        MarketWebApiTokenSource: "steamcommunity_webapi"
    );

    Log("createSession:done", new
    {
        profile_name = profileName,
        steamid64 = item.SteamId64,
        has_cookies = !string.IsNullOrWhiteSpace(item.SteamWebCookie),
    });

    return item;
}

void DestroySession(string profileName, string reason)
{
    sessions.TryRemove(profileName, out _);
    restoredSessionsNeedingCheck.TryRemove(profileName, out _);
    SaveSessionsToDisk();

    Log("session:destroy", new
    {
        profile_name = profileName,
        reason,
    });
}

void SaveSessionsToDisk()
{
    lock (sessionStoreLock)
    {
        try
        {
            var items = sessions.Values
                .Select(x => new PersistedSession(
                    ProfileName: x.ProfileName,
                    Proxy: x.Proxy,
                    Fingerprint: x.Fingerprint,
                    SteamId64: x.SteamId64,
                    SteamWebCookie: x.SteamWebCookie,
                    MarketWebApiToken: x.MarketWebApiToken,
                    RefreshToken: x.RefreshToken,
                    IdentitySecret: x.IdentitySecret,
                    DeviceId: x.DeviceId,
                    CreatedAtUtc: x.CreatedAtUtc,
                    WebCookieExpiresAtUtc: x.WebCookieExpiresAtUtc,
                    MarketWebApiTokenExpiresAtUtc: x.MarketWebApiTokenExpiresAtUtc,
                    RefreshTokenExpiresAtUtc: x.RefreshTokenExpiresAtUtc,
                    MarketWebApiTokenSource: x.MarketWebApiTokenSource
                ))
                .ToList();

            var json = JsonSerializer.Serialize(items, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(sessionStorePath, json, Encoding.UTF8);

            Log("session_store:save", new
            {
                path = sessionStorePath,
                count = items.Count,
            });
        }
        catch (Exception ex)
        {
            Log("session_store:save_error", new
            {
                path = sessionStorePath,
                error = ex.ToString(),
            });
        }
    }
}

void LoadSessionsFromDisk()
{
    lock (sessionStoreLock)
    {
        try
        {
            if (!File.Exists(sessionStorePath))
            {
                Log("session_store:load", new
                {
                    path = sessionStorePath,
                    count = 0,
                    exists = false,
                });
                return;
            }

            var json = File.ReadAllText(sessionStorePath, Encoding.UTF8);
            if (string.IsNullOrWhiteSpace(json))
            {
                return;
            }

            var items = JsonSerializer.Deserialize<List<PersistedSession>>(json) ?? new List<PersistedSession>();
            var now = DateTimeOffset.UtcNow;
            var loaded = 0;

            foreach (var x in items)
            {
                if (string.IsNullOrWhiteSpace(x.ProfileName) ||
                    string.IsNullOrWhiteSpace(x.Proxy) ||
                    string.IsNullOrWhiteSpace(x.Fingerprint) ||
                    string.IsNullOrWhiteSpace(x.SteamId64) ||
                    string.IsNullOrWhiteSpace(x.SteamWebCookie) ||
                    string.IsNullOrWhiteSpace(x.IdentitySecret))
                {
                    continue;
                }

                var hasUsableWebCookie = !string.IsNullOrWhiteSpace(x.SteamWebCookie) && x.WebCookieExpiresAtUtc > now.AddMinutes(2);
                var hasUsableRefreshToken = !string.IsNullOrWhiteSpace(x.RefreshToken) && x.RefreshTokenExpiresAtUtc > now.AddMinutes(5);
                if (!hasUsableWebCookie && !hasUsableRefreshToken)
                {
                    continue;
                }

                var restored = new CachedSession(
                    ProfileName: x.ProfileName,
                    Proxy: x.Proxy,
                    Fingerprint: x.Fingerprint,
                    SteamId64: x.SteamId64,
                    SteamWebCookie: x.SteamWebCookie ?? string.Empty,
                    MarketWebApiToken: x.MarketWebApiToken ?? string.Empty,
                    RefreshToken: x.RefreshToken ?? string.Empty,
                    IdentitySecret: x.IdentitySecret,
                    DeviceId: x.DeviceId ?? string.Empty,
                    CreatedAtUtc: x.CreatedAtUtc,
                    WebCookieExpiresAtUtc: x.WebCookieExpiresAtUtc,
                    MarketWebApiTokenExpiresAtUtc: x.MarketWebApiTokenExpiresAtUtc,
                    RefreshTokenExpiresAtUtc: x.RefreshTokenExpiresAtUtc,
                    MarketWebApiTokenSource: string.IsNullOrWhiteSpace(x.MarketWebApiTokenSource) ? "steamcommunity_webapi" : x.MarketWebApiTokenSource
                );

                sessions[x.ProfileName] = restored;
                restoredSessionsNeedingCheck[x.ProfileName] = 1;
                loaded++;
            }

            Log("session_store:load", new
            {
                path = sessionStorePath,
                count = loaded,
                exists = true,
            });
        }
        catch (Exception ex)
        {
            Log("session_store:load_error", new
            {
                path = sessionStorePath,
                error = ex.ToString(),
            });
        }
    }
}

static string GetProfileName(AccessTokenRequest request)
{
    var profileName = string.IsNullOrWhiteSpace(request.ProfileName)
        ? request.Name?.Trim() ?? string.Empty
        : request.ProfileName.Trim();

    if (string.IsNullOrWhiteSpace(profileName))
    {
        throw new InvalidOperationException("profile_name is required");
    }

    return profileName;
}

static object MaskProfile(AccessTokenRequest request)
{
    return new
    {
        profile_name = string.IsNullOrWhiteSpace(request.ProfileName) ? request.Name?.Trim() ?? string.Empty : request.ProfileName.Trim(),
        proxy = string.IsNullOrWhiteSpace(request.Proxy) ? "no" : "yes",
        steam_username = string.IsNullOrWhiteSpace(request.SteamUsername) ? "no" : "yes",
        steam_password = string.IsNullOrWhiteSpace(request.SteamPassword) ? "no" : "yes",
        mafile_path = string.IsNullOrWhiteSpace(request.MafilePath) ? "no" : "yes",
        mafile_data = request.MafileData.HasValue && request.MafileData.Value.ValueKind != JsonValueKind.Null ? "yes" : "no",
    };
}

static string BuildSessionFingerprint(AccessTokenRequest request, string normalizedProxy)
{
    return JsonSerializer.Serialize(new
    {
        proxy = normalizedProxy,
        steam_username = (request.SteamUsername ?? string.Empty).Trim(),
        mafile_path = (request.MafilePath ?? string.Empty).Trim(),
        has_mafile_data = request.MafileData.HasValue && request.MafileData.Value.ValueKind != JsonValueKind.Null,
    });
}

static string RequireProxy(string? proxy)
{
    var raw = (proxy ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(raw))
    {
        throw new InvalidOperationException("Steam proxy is required");
    }

    return NormalizeProxy(raw);
}

static string NormalizeProxy(string proxy)
{
    var p = proxy.Trim();
    if (string.IsNullOrWhiteSpace(p))
    {
        return string.Empty;
    }

    if (p.Contains("://", StringComparison.Ordinal))
    {
        if (!Uri.TryCreate(p, UriKind.Absolute, out var absolute) || string.IsNullOrWhiteSpace(absolute.Host) || absolute.Port <= 0)
        {
            throw new InvalidOperationException("Invalid proxy format");
        }

        return absolute.ToString();
    }

    var parts = p.Split(':');
    if (parts.Length == 2)
    {
        var host = parts[0];
        var port = parts[1];
        if (string.IsNullOrWhiteSpace(host) || string.IsNullOrWhiteSpace(port))
        {
            throw new InvalidOperationException("Invalid proxy format");
        }

        return $"http://{host}:{port}";
    }

    if (parts.Length == 4)
    {
        var host = parts[0];
        var port = parts[1];
        var user = parts[2];
        var pass = parts[3];

        if (string.IsNullOrWhiteSpace(host) || string.IsNullOrWhiteSpace(port) || string.IsNullOrWhiteSpace(user) || string.IsNullOrWhiteSpace(pass))
        {
            throw new InvalidOperationException("Invalid proxy format");
        }

        return $"http://{Uri.EscapeDataString(user)}:{Uri.EscapeDataString(pass)}@{host}:{port}";
    }

    throw new InvalidOperationException("Invalid proxy format");
}

static async Task<string> TestProxyOrThrowAsync(string proxyUrl)
{
    using var http = CreateProxiedHttpClient(proxyUrl, TimeSpan.FromSeconds(10));
    using var response = await http.GetAsync("https://api.ipify.org?format=json").ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"Proxy test failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
    }

    using var doc = JsonDocument.Parse(body);
    var ip = TryGetString(doc.RootElement, "ip");
    if (string.IsNullOrWhiteSpace(ip))
    {
        throw new InvalidOperationException("proxy returned empty IP");
    }

    return ip;
}

static async Task WarmupInventoryAsync(string proxyUrl, string steamId64, string steamWebCookie)
{
    using var http = CreateProxiedHttpClient(proxyUrl, TimeSpan.FromSeconds(20));
    using var request = new HttpRequestMessage(HttpMethod.Get, $"https://steamcommunity.com/profiles/{steamId64}/inventory");
    request.Headers.TryAddWithoutValidation("Cookie", BuildSteamCookieHeader(steamId64, steamWebCookie));
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
    request.Headers.TryAddWithoutValidation("Accept-Language", "en-US,en;q=0.9");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    _ = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
}

static bool IsTokenStillFresh(string? token, DateTimeOffset expiresAtUtc, int minRemainingSeconds = 120)
{
    if (string.IsNullOrWhiteSpace(token))
    {
        return false;
    }

    var effectiveExpiry = expiresAtUtc == default
        ? TryReadJwtExpiry(token) ?? DateTimeOffset.MinValue
        : expiresAtUtc;

    return effectiveExpiry > DateTimeOffset.UtcNow.AddSeconds(minRemainingSeconds);
}

static bool IsAjaxGetAsyncConfigTokenError(Exception ex)
{
    var message = ex.Message ?? string.Empty;
    return message.Contains("webapi_token not found in ajaxgetasyncconfig response", StringComparison.OrdinalIgnoreCase)
        || message.Contains("SteamCommunity ajaxgetasyncconfig failed", StringComparison.OrdinalIgnoreCase);
}

static async Task<GeneratedAccessTokenResult> GenerateAccessTokenFromRefreshTokenAsync(string proxyUrl, string steamId64, string refreshToken, bool allowRenewal)
{
    if (string.IsNullOrWhiteSpace(refreshToken))
    {
        throw new InvalidOperationException("refresh token is empty");
    }

    using var http = CreateProxiedHttpClient(proxyUrl, TimeSpan.FromSeconds(30));
    using var request = new HttpRequestMessage(HttpMethod.Post, "https://api.steampowered.com/IAuthenticationService/GenerateAccessTokenForApp/v1/");
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");
    request.Headers.TryAddWithoutValidation("Accept-Language", "en-US,en;q=0.9");
    request.Headers.TryAddWithoutValidation("Referer", "https://steamcommunity.com/");
    request.Headers.TryAddWithoutValidation("Origin", "https://steamcommunity.com");

    var form = new List<KeyValuePair<string, string>>
    {
        new("steamid", steamId64),
        new("refresh_token", refreshToken),
    };

    if (allowRenewal)
    {
        form.Add(new KeyValuePair<string, string>("renewal_type", "1"));
    }

    request.Content = new FormUrlEncodedContent(form);

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"GenerateAccessTokenForApp failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}; body={(body.Length > 500 ? body[..500] : body)}");
    }

    using var doc = JsonDocument.Parse(body);
    var accessToken = TryGetNestedString(doc.RootElement, "response", "access_token") ?? TryGetString(doc.RootElement, "access_token");
    var newRefreshToken = TryGetNestedString(doc.RootElement, "response", "refresh_token") ?? TryGetString(doc.RootElement, "refresh_token");

    if (string.IsNullOrWhiteSpace(accessToken))
    {
        throw new InvalidOperationException($"GenerateAccessTokenForApp returned empty access_token; body={(body.Length > 700 ? body[..700] : body)}");
    }

    accessToken = accessToken.Trim();
    newRefreshToken = string.IsNullOrWhiteSpace(newRefreshToken) ? refreshToken.Trim() : newRefreshToken.Trim();

    return new GeneratedAccessTokenResult(
        AccessToken: accessToken,
        RefreshToken: newRefreshToken,
        AccessTokenExpiresAtUtc: TryReadJwtExpiry(accessToken) ?? DateTimeOffset.UtcNow.AddHours(24),
        RefreshTokenExpiresAtUtc: TryReadJwtExpiry(newRefreshToken) ?? DateTimeOffset.UtcNow.AddDays(180)
    );
}

async Task<CachedSession> RefreshSessionFromStoredRefreshTokenAsync(AccessTokenRequest request, CachedSession session, string reason, bool allowRenewal)
{
    var profileName = GetProfileName(request);

    if (string.IsNullOrWhiteSpace(session.RefreshToken))
    {
        throw new InvalidOperationException("refresh token is missing for this session");
    }

    Log("access-token:refresh_via_refresh_token:start", new
    {
        profile_name = profileName,
        reason,
        allow_renewal = allowRenewal,
    });

    var generated = await GenerateAccessTokenFromRefreshTokenAsync(
        session.Proxy,
        session.SteamId64,
        session.RefreshToken,
        allowRenewal
    ).ConfigureAwait(false);

    var refreshedSession = session with
    {
        SteamWebCookie = generated.AccessToken,
        WebCookieExpiresAtUtc = generated.AccessTokenExpiresAtUtc,
        RefreshToken = generated.RefreshToken,
        RefreshTokenExpiresAtUtc = generated.RefreshTokenExpiresAtUtc,
    };

    string marketToken;
    DateTimeOffset marketTokenExpiresAtUtc;
    string marketTokenSource;

    try
    {
        marketToken = await FetchSteamCommunityWebApiTokenAsync(
            refreshedSession.Proxy,
            refreshedSession.SteamId64,
            refreshedSession.SteamWebCookie
        ).ConfigureAwait(false);

        marketTokenExpiresAtUtc = TryReadJwtExpiry(marketToken) ?? generated.AccessTokenExpiresAtUtc;
        marketTokenSource = "steamcommunity_webapi_after_refresh";
    }
    catch (Exception ex) when (IsAjaxGetAsyncConfigTokenError(ex))
    {
        marketToken = generated.AccessToken;
        marketTokenExpiresAtUtc = generated.AccessTokenExpiresAtUtc;
        marketTokenSource = "refresh_token_mobile_access";

        Log("access-token:refresh_via_refresh_token:fallback_mobile_access", new
        {
            profile_name = profileName,
            error = ex.Message,
        });
    }

    refreshedSession = refreshedSession with
    {
        MarketWebApiToken = marketToken,
        MarketWebApiTokenExpiresAtUtc = marketTokenExpiresAtUtc,
        MarketWebApiTokenSource = marketTokenSource,
    };

    sessions[profileName] = refreshedSession;
    SaveSessionsToDisk();

    Log("access-token:refresh_via_refresh_token:ok", new
    {
        profile_name = profileName,
        source = refreshedSession.MarketWebApiTokenSource,
        refresh_token_rotated = !string.Equals(session.RefreshToken, refreshedSession.RefreshToken, StringComparison.Ordinal),
    });

    return refreshedSession;
}

async Task<CachedSession> EnsureRestoredSessionReadyAsync(AccessTokenRequest request, CachedSession cached)
{
    var profileName = GetProfileName(request);

    if (IsTokenStillFresh(cached.SteamWebCookie, cached.WebCookieExpiresAtUtc, 120))
    {
        return cached;
    }

    if (!string.IsNullOrWhiteSpace(cached.RefreshToken))
    {
        Log("ensureSession:restored_refresh_needed", new
        {
            profile_name = profileName,
        });

        return await RefreshSessionFromStoredRefreshTokenAsync(request, cached, "restored_session", allowRenewal: true).ConfigureAwait(false);
    }

    throw new InvalidOperationException("restored session has expired web cookie and no refresh token");
}

async Task<CachedSession> EnsureAccessTokenReadyAsync(AccessTokenRequest request, CachedSession session)
{
    var profileName = GetProfileName(request);

    if (IsTokenStillFresh(session.MarketWebApiToken, session.MarketWebApiTokenExpiresAtUtc, 120))
    {
        return session;
    }

    if (IsTokenStillFresh(session.SteamWebCookie, session.WebCookieExpiresAtUtc, 120))
    {
        try
        {
            var refreshedMarketToken = await FetchSteamCommunityWebApiTokenAsync(
                session.Proxy,
                session.SteamId64,
                session.SteamWebCookie
            ).ConfigureAwait(false);

            var refreshed = session with
            {
                MarketWebApiToken = refreshedMarketToken,
                MarketWebApiTokenExpiresAtUtc = TryReadJwtExpiry(refreshedMarketToken) ?? session.WebCookieExpiresAtUtc,
                MarketWebApiTokenSource = "steamcommunity_webapi",
            };

            sessions[profileName] = refreshed;
            SaveSessionsToDisk();
            return refreshed;
        }
        catch (Exception ex) when (IsAjaxGetAsyncConfigTokenError(ex) && !string.IsNullOrWhiteSpace(session.RefreshToken))
        {
            Log("access-token:ajaxgetasyncconfig_failed", new
            {
                profile_name = profileName,
                error = ex.Message,
            });

            return await RefreshSessionFromStoredRefreshTokenAsync(request, session, "ajaxgetasyncconfig_failed", allowRenewal: true).ConfigureAwait(false);
        }
    }

    if (!string.IsNullOrWhiteSpace(session.RefreshToken))
    {
        return await RefreshSessionFromStoredRefreshTokenAsync(request, session, "cached_access_token_expired", allowRenewal: true).ConfigureAwait(false);
    }

    throw new InvalidOperationException("market access token expired and no refresh token is available");
}

static async Task<string> FetchSteamCommunityWebApiTokenAsync(string proxyUrl, string steamId64, string steamWebCookie)
{
    using var http = CreateProxiedHttpClient(proxyUrl, TimeSpan.FromSeconds(30));
    using var request = new HttpRequestMessage(HttpMethod.Get, "https://steamcommunity.com/pointssummary/ajaxgetasyncconfig");

    request.Headers.TryAddWithoutValidation("Cookie", BuildSteamCookieHeader(steamId64, steamWebCookie));
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");
    request.Headers.TryAddWithoutValidation("Accept-Language", "en-US,en;q=0.9");
    request.Headers.TryAddWithoutValidation("Referer", $"https://steamcommunity.com/profiles/{steamId64}/inventory");
    request.Headers.TryAddWithoutValidation("Origin", "https://steamcommunity.com");
    request.Headers.TryAddWithoutValidation("X-Requested-With", "XMLHttpRequest");
    request.Headers.TryAddWithoutValidation("Connection", "keep-alive");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"SteamCommunity ajaxgetasyncconfig failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}; body={(body.Length > 400 ? body[..400] : body)}");
    }

    using var doc = JsonDocument.Parse(body);

    var token = TryGetNestedString(doc.RootElement, "data", "webapi_token")
                ?? TryGetString(doc.RootElement, "webapi_token");

    if (string.IsNullOrWhiteSpace(token))
    {
        throw new InvalidOperationException(
            $"webapi_token not found in ajaxgetasyncconfig response; body={(body.Length > 700 ? body[..700] : body)}"
        );
    }

    return token.Trim();
}

static async Task<List<SteamInventoryItem>> FetchFullSteamInventoryAsync(string proxyUrl, string steamId64, string steamWebCookie, int count)
{
    // 1) обычный кейс: сначала пытаемся через context 2
    var items = await FetchFullSteamInventoryForContextAsync(
        proxyUrl,
        steamId64,
        steamWebCookie,
        count,
        "2"
    ).ConfigureAwait(false);

    if (items.Count > 0)
    {
        return items;
    }

    // 2) fallback: если 2 пустой — пробуем 16
    Log("steam/inventory:fallback_context", new
    {
        steamid64 = steamId64,
        from_context = "2",
        to_context = "16",
    });

    items = await FetchFullSteamInventoryForContextAsync(
        proxyUrl,
        steamId64,
        steamWebCookie,
        count,
        "16"
    ).ConfigureAwait(false);

    return items;
}

static async Task<List<SteamInventoryItem>> FetchFullSteamInventoryForContextAsync(
    string proxyUrl,
    string steamId64,
    string steamWebCookie,
    int count,
    string contextId)
{
    var allItems = new List<SteamInventoryItem>();
    string? startAssetId = null;

    for (var page = 0; page < 10; page++)
    {
        var result = await FetchSteamInventoryPageAsync(
            proxyUrl,
            steamId64,
            steamWebCookie,
            count,
            startAssetId,
            contextId
        ).ConfigureAwait(false);

        if (result.Items.Count > 0)
        {
            allItems.AddRange(result.Items);
        }

        if (!result.MoreItems || string.IsNullOrWhiteSpace(result.LastAssetId))
        {
            break;
        }

        startAssetId = result.LastAssetId;
    }

    Log("steam/inventory:context_done", new
    {
        steamid64 = steamId64,
        contextid = contextId,
        count = allItems.Count,
    });

    return allItems;
}

static async Task<SteamInventoryPageResult> FetchSteamInventoryPageAsync(
    string proxyUrl,
    string steamId64,
    string steamWebCookie,
    int count,
    string? startAssetId,
    string contextId)
{
    using var http = CreateProxiedHttpClient(proxyUrl, TimeSpan.FromSeconds(45));

    var url = $"https://steamcommunity.com/inventory/{steamId64}/730/{contextId}?l=english&count={count}";
    if (!string.IsNullOrWhiteSpace(startAssetId))
    {
        url += $"&start_assetid={Uri.EscapeDataString(startAssetId)}";
    }

    using var request = new HttpRequestMessage(HttpMethod.Get, url);
    request.Headers.TryAddWithoutValidation("Cookie", BuildSteamCookieHeader(steamId64, steamWebCookie));
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");
    request.Headers.TryAddWithoutValidation("Accept-Language", "en-US,en;q=0.9");
    request.Headers.TryAddWithoutValidation("Referer", $"https://steamcommunity.com/profiles/{steamId64}/inventory");
    request.Headers.TryAddWithoutValidation("X-Requested-With", "XMLHttpRequest");
    request.Headers.TryAddWithoutValidation("Connection", "keep-alive");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException(
            $"Steam inventory failed: context={contextId}; HTTP {(int)response.StatusCode} {response.ReasonPhrase}; body={(body.Length > 400 ? body[..400] : body)}"
        );
    }

    using var doc = JsonDocument.Parse(body);
    var root = doc.RootElement;

    var descriptions = new Dictionary<string, SteamInventoryDescription>(StringComparer.Ordinal);
    if (root.TryGetProperty("descriptions", out var descriptionsElement) && descriptionsElement.ValueKind == JsonValueKind.Array)
    {
        foreach (var desc in descriptionsElement.EnumerateArray())
        {
            var classId = TryGetString(desc, "classid") ?? string.Empty;
            var instanceId = TryGetString(desc, "instanceid") ?? string.Empty;
            if (string.IsNullOrWhiteSpace(classId))
            {
                continue;
            }

            var key = $"{classId}_{instanceId}";
            descriptions[key] = new SteamInventoryDescription(
                MarketHashName: TryGetString(desc, "market_hash_name") ?? TryGetString(desc, "name") ?? string.Empty,
                Name: TryGetString(desc, "name") ?? TryGetString(desc, "market_hash_name") ?? string.Empty,
                Tradable: ParseIntSafe(TryGetString(desc, "tradable")),
                Marketable: ParseIntSafe(TryGetString(desc, "marketable"))
            );
        }
    }

    var items = new List<SteamInventoryItem>();
    if (root.TryGetProperty("assets", out var assetsElement) && assetsElement.ValueKind == JsonValueKind.Array)
    {
        foreach (var asset in assetsElement.EnumerateArray())
        {
            var classId = TryGetString(asset, "classid") ?? string.Empty;
            var instanceId = TryGetString(asset, "instanceid") ?? string.Empty;
            var key = $"{classId}_{instanceId}";
            descriptions.TryGetValue(key, out var desc);

            items.Add(new SteamInventoryItem
            {
                AssetId = TryGetString(asset, "assetid"),
                ClassId = classId,
                InstanceId = instanceId,
                ContextId = TryGetString(asset, "contextid") ?? contextId,
                Amount = ParseIntSafe(TryGetString(asset, "amount")),
                MarketHashName = desc?.MarketHashName ?? string.Empty,
                Name = desc?.Name ?? string.Empty,
                Tradable = desc?.Tradable ?? 0,
                Marketable = desc?.Marketable ?? 0,
            });
        }
    }

    var moreItems = false;
    if (root.TryGetProperty("more_items", out var moreItemsElement))
    {
        moreItems = moreItemsElement.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number => moreItemsElement.GetInt32() != 0,
            _ => false,
        };
    }

    var lastAssetId = TryGetString(root, "last_assetid");

    Log("steam/inventory:page", new
    {
        steamid64 = steamId64,
        contextid = contextId,
        page_items = items.Count,
        more_items = moreItems,
        last_assetid = lastAssetId,
    });

    return new SteamInventoryPageResult(items, moreItems, lastAssetId);
}

static int ParseIntSafe(string? raw)
{
    return int.TryParse(raw, out var value) ? value : 0;
}

static long ParseLongSafe(string? raw)
{
    return long.TryParse(raw, out var value) ? value : 0;
}

static bool ParseBoolJson(JsonElement element, string propertyName)
{
    if (!element.TryGetProperty(propertyName, out var value))
    {
        return false;
    }

    return value.ValueKind switch
    {
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Number => value.TryGetInt32(out var number) && number != 0,
        JsonValueKind.String => bool.TryParse(value.GetString(), out var b) ? b : (int.TryParse(value.GetString(), out var n) && n != 0),
        _ => false,
    };
}

static HttpClient CreateProxiedHttpClient(string proxyUrl, TimeSpan timeout)
{
    var handler = new HttpClientHandler
    {
        Proxy = BuildProxy(proxyUrl),
        UseProxy = true,
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        UseCookies = false,
    };

    return new HttpClient(handler)
    {
        Timeout = timeout,
    };
}

static HttpClient CreateProxiedHttpClientWithCookies(string proxyUrl, TimeSpan timeout, CookieContainer? cookieContainer = null)
{
    var handler = new HttpClientHandler
    {
        Proxy = BuildProxy(proxyUrl),
        UseProxy = true,
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        UseCookies = true,
        CookieContainer = cookieContainer ?? new CookieContainer(),
    };

    return new HttpClient(handler)
    {
        Timeout = timeout,
    };
}

static HttpClient CreateSteamCommunityWebClient(CachedSession session, TimeSpan timeout, out CookieContainer cookieJar, out string sessionId)
{
    cookieJar = new CookieContainer();
    sessionId = Convert.ToHexString(RandomNumberGenerator.GetBytes(12)).ToLowerInvariant();
    SeedSteamCommunityCookies(cookieJar, session, sessionId);

    return CreateProxiedHttpClientWithCookies(session.Proxy, timeout, cookieJar);
}

static void SeedSteamCommunityCookies(CookieContainer cookieJar, CachedSession session, string sessionId)
{
    var communityUri = new Uri("https://steamcommunity.com/");
    var steamLoginSecureValue = Uri.EscapeDataString($"{session.SteamId64}||{session.SteamWebCookie}");

    cookieJar.Add(communityUri, new Cookie("steamLoginSecure", steamLoginSecureValue, "/", "steamcommunity.com")
    {
        Secure = true,
        HttpOnly = true,
    });

    cookieJar.Add(communityUri, new Cookie("steamRememberLogin", "true", "/", "steamcommunity.com")
    {
        Secure = true,
        HttpOnly = false,
    });

    cookieJar.Add(communityUri, new Cookie("sessionid", sessionId, "/", "steamcommunity.com")
    {
        Secure = true,
        HttpOnly = false,
    });
}

static void UpsertSteamCommunitySessionId(CookieContainer cookieJar, string sessionId)
{
    var communityUri = new Uri("https://steamcommunity.com/");
    cookieJar.Add(communityUri, new Cookie("sessionid", sessionId, "/", "steamcommunity.com")
    {
        Secure = true,
        HttpOnly = false,
    });
}

static string GetTradeOfferPagePartnerParam(string partnerRaw)
{
    var partner = (partnerRaw ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(partner))
    {
        throw new InvalidOperationException("offer.partner is required");
    }

    if (ulong.TryParse(partner, out var partner64))
    {
        try
        {
            var sid = new SteamID(partner64);
            if (sid.IsIndividualAccount)
            {
                return sid.AccountID.ToString();
            }
        }
        catch
        {
            // keep original partner string below
        }
    }

    return partner;
}

static string GetTradeOfferPostPartnerParam(string partnerRaw)
{
    var partner = (partnerRaw ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(partner))
    {
        throw new InvalidOperationException("offer.partner is required");
    }

    if (!ulong.TryParse(partner, out var parsed))
    {
        throw new InvalidOperationException("offer.partner must be numeric");
    }

    // уже SteamID64
    if (parsed > 76561197960265728UL)
    {
        return parsed.ToString();
    }

    // accountid32 -> SteamID64
    var sid = new SteamID((uint)parsed, EUniverse.Public, EAccountType.Individual);
    return sid.ConvertToUInt64().ToString();
}

static string? TryExtractSessionIdFromHtml(string html)
{
    if (string.IsNullOrWhiteSpace(html))
    {
        return null;
    }

    var match = Regex.Match(html, @"g_sessionID\s*=\s*""([^""]+)""", RegexOptions.IgnoreCase);
    if (match.Success)
    {
        return match.Groups[1].Value.Trim();
    }

    match = Regex.Match(html, @"sessionid""\s*:\s*""([^""]+)""", RegexOptions.IgnoreCase);
    if (match.Success)
    {
        return match.Groups[1].Value.Trim();
    }

    return null;
}

static async Task<string> PreflightTradeOfferPageAsync(CachedSession session, HttpClient http, CookieContainer cookieJar, string partnerPageParam, string token)
{
    var url = $"https://steamcommunity.com/tradeoffer/new/?partner={Uri.EscapeDataString(partnerPageParam)}";
    if (!string.IsNullOrWhiteSpace(token))
    {
        url += $"&token={Uri.EscapeDataString(token)}";
    }

    using var request = new HttpRequestMessage(HttpMethod.Get, url);
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
    request.Headers.TryAddWithoutValidation("Accept-Language", "en-US,en;q=0.9");
    request.Headers.TryAddWithoutValidation("Cache-Control", "no-cache");
    request.Headers.TryAddWithoutValidation("Pragma", "no-cache");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

    Log("tradeoffer/send:preflight", new
    {
        profile_name = session.ProfileName,
        status = (int)response.StatusCode,
        reason = response.ReasonPhrase,
        final_uri = response.RequestMessage?.RequestUri?.ToString(),
        body_head = body.Length > 220 ? body[..220] : body,
        cookies = cookieJar.GetCookieHeader(new Uri("https://steamcommunity.com/")),
    });

    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"tradeoffer preflight failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}; body={(body.Length > 700 ? body[..700] : body)}");
    }

    if (body.Contains("loginForm", StringComparison.OrdinalIgnoreCase) ||
        body.Contains("OpenID", StringComparison.OrdinalIgnoreCase) ||
        body.Contains("Sign In", StringComparison.OrdinalIgnoreCase))
    {
        throw new InvalidOperationException("tradeoffer preflight returned login page");
    }

    return body;
}

static IWebProxy BuildProxy(string proxyUrl)
{
    var uri = new Uri(proxyUrl);

    var cleanProxyUri = $"{uri.Scheme}://{uri.Host}:{uri.Port}";
    var proxy = new WebProxy(cleanProxyUri);

    if (!string.IsNullOrWhiteSpace(uri.UserInfo))
    {
        var parts = uri.UserInfo.Split(':', 2);
        var user = parts.Length > 0 ? Uri.UnescapeDataString(parts[0]) : "";
        var pass = parts.Length > 1 ? Uri.UnescapeDataString(parts[1]) : "";

        proxy.Credentials = new NetworkCredential(user, pass);
    }

    return proxy;
}

static string BuildSteamCookieHeader(string steamId64, string accessToken, string? sessionId = null)
{
    sessionId ??= Convert.ToHexString(RandomNumberGenerator.GetBytes(12)).ToLowerInvariant();

    var steamLoginSecureValue = Uri.EscapeDataString($"{steamId64}||{accessToken}");

    return $"steamLoginSecure={steamLoginSecureValue}; sessionid={sessionId}; steamRememberLogin=true";
}

static DateTimeOffset? TryReadJwtExpiry(string jwt)
{
    try
    {
        var parts = jwt.Split('.');
        if (parts.Length < 2)
        {
            return null;
        }

        var payload = parts[1]
            .Replace('-', '+')
            .Replace('_', '/');

        switch (payload.Length % 4)
        {
            case 2:
                payload += "==";
                break;
            case 3:
                payload += "=";
                break;
            case 1:
                payload += "===";
                break;
        }

        var json = Encoding.UTF8.GetString(Convert.FromBase64String(payload));
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.TryGetProperty("exp", out var expElement) && expElement.TryGetInt64(out var exp))
        {
            return DateTimeOffset.FromUnixTimeSeconds(exp);
        }
    }
    catch
    {
        // ignore malformed JWT parsing
    }

    return null;
}

static string RequireTradeOfferId(string? tradeOfferId)
{
    var raw = (tradeOfferId ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(raw))
    {
        throw new InvalidOperationException("tradeoffer_id is required");
    }

    return raw;
}

static string GenerateDeviceId(string steamId64)
{
    var raw = SHA1.HashData(Encoding.ASCII.GetBytes(steamId64));
    var hex = Convert.ToHexString(raw).ToLowerInvariant();
    return $"android:{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..32]}";
}

static string GenerateConfirmationKey(string identitySecret, string tag, long timestamp)
{
    var secret = Convert.FromBase64String(identitySecret);
    var tagBytes = Encoding.UTF8.GetBytes(tag ?? string.Empty);
    var buffer = new byte[8 + Math.Min(32, tagBytes.Length)];

    for (var i = 7; i >= 0; i--)
    {
        buffer[i] = (byte)timestamp;
        timestamp >>= 8;
    }

    if (tagBytes.Length > 0)
    {
        Array.Copy(tagBytes, 0, buffer, 8, Math.Min(32, tagBytes.Length));
    }

    using var hmac = new HMACSHA1(secret);
    return Uri.EscapeDataString(Convert.ToBase64String(hmac.ComputeHash(buffer)));
}

static async Task<TradeOfferSendInternalResult> SendTradeOfferAsync(CachedSession session, JsonElement offer)
{
    var partner = TryGetString(offer, "partner") ?? throw new InvalidOperationException("offer.partner is required");
    var partnerPageParam = GetTradeOfferPagePartnerParam(partner);
    var partnerPostParam = GetTradeOfferPostPartnerParam(partner);
    var token = (TryGetString(offer, "token") ?? string.Empty).Trim();
    var tradeOfferMessage = TryGetString(offer, "tradeoffermessage") ?? string.Empty;

    if (!offer.TryGetProperty("items", out var itemsElement) || itemsElement.ValueKind != JsonValueKind.Array)
    {
        throw new InvalidOperationException("offer.items is required");
    }

    var myAssets = new List<object>();
    var debugItems = new List<object>();
    foreach (var item in itemsElement.EnumerateArray())
    {
        var appId = ParseIntSafe(TryGetString(item, "appid"));
        var contextId = TryGetString(item, "contextid") ?? "2";
        var assetId = TryGetString(item, "assetid") ?? throw new InvalidOperationException("offer.items[].assetid is required");
        var amount = ParseIntSafe(TryGetString(item, "amount")) is var amt && amt > 0 ? amt : 1;

        myAssets.Add(new
        {
            appid = appId,
            contextid = contextId,
            assetid = assetId,
            amount = amount,
        });

        debugItems.Add(new
        {
            appid = appId,
            contextid = contextId,
            assetid = assetId,
            amount = amount,
        });
    }

    if (myAssets.Count == 0)
    {
        throw new InvalidOperationException("offer.items is empty");
    }

    var tradePayload = JsonSerializer.Serialize(new
    {
        newversion = true,
        version = 3,
        me = new { assets = myAssets, currency = Array.Empty<object>(), ready = false },
        them = new { assets = Array.Empty<object>(), currency = Array.Empty<object>(), ready = false },
    });

    var tradeOfferCreateParams = string.IsNullOrWhiteSpace(token)
        ? "{}"
        : JsonSerializer.Serialize(new { trade_offer_access_token = token });

    using var http = CreateSteamCommunityWebClient(session, TimeSpan.FromSeconds(45), out var cookieJar, out var sessionId);

    Log("tradeoffer/send:prepared", new
    {
        profile_name = session.ProfileName,
        partner_post = partnerPostParam,
        partner_page = partnerPageParam,
        partner_raw = partner,
        token_len = token.Length,
        item_count = myAssets.Count,
        items = debugItems,
        sessionid = sessionId,
    });

    var preflightHtml = await PreflightTradeOfferPageAsync(session, http, cookieJar, partnerPageParam, token).ConfigureAwait(false);
    var htmlSessionId = TryExtractSessionIdFromHtml(preflightHtml);
    if (!string.IsNullOrWhiteSpace(htmlSessionId) && !string.Equals(htmlSessionId, sessionId, StringComparison.Ordinal))
    {
        sessionId = htmlSessionId;
        UpsertSteamCommunitySessionId(cookieJar, sessionId);

        Log("tradeoffer/send:sessionid_updated", new
        {
            profile_name = session.ProfileName,
            sessionid = sessionId,
        });
    }

    var referer = $"https://steamcommunity.com/tradeoffer/new/?partner={Uri.EscapeDataString(partnerPageParam)}";
    if (!string.IsNullOrWhiteSpace(token))
    {
        referer += $"&token={Uri.EscapeDataString(token)}";
    }

    using var request = new HttpRequestMessage(HttpMethod.Post, "https://steamcommunity.com/tradeoffer/new/send");
    request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
    {
        ["sessionid"] = sessionId,
        ["serverid"] = "1",
        ["partner"] = partnerPostParam,
        ["tradeoffermessage"] = tradeOfferMessage,
        ["json_tradeoffer"] = tradePayload,
        ["captcha"] = string.Empty,
        ["trade_offer_create_params"] = tradeOfferCreateParams,
    });

    request.Headers.TryAddWithoutValidation("Referer", referer);
    request.Headers.TryAddWithoutValidation("Origin", "https://steamcommunity.com");
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("Accept", "application/json, text/javascript, */*; q=0.01");
    request.Headers.TryAddWithoutValidation("Accept-Language", "en-US,en;q=0.9");
    request.Headers.TryAddWithoutValidation("X-Requested-With", "XMLHttpRequest");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

    Log("tradeoffer/send:response", new
    {
        profile_name = session.ProfileName,
        status = (int)response.StatusCode,
        reason = response.ReasonPhrase,
        body = body.Length > 700 ? body[..700] : body,
        cookies = cookieJar.GetCookieHeader(new Uri("https://steamcommunity.com/")),
    });

    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"tradeoffer send failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}; body={(body.Length > 700 ? body[..700] : body)}");
    }

    using var doc = JsonDocument.Parse(body);
    var root = doc.RootElement;
    var error = TryGetString(root, "strError");
    if (!string.IsNullOrWhiteSpace(error))
    {
        throw new InvalidOperationException(error);
    }

    var tradeOfferId = TryGetString(root, "tradeofferid");
    if (string.IsNullOrWhiteSpace(tradeOfferId))
    {
        throw new InvalidOperationException($"tradeofferid not found in response; body={(body.Length > 700 ? body[..700] : body)}");
    }

    return new TradeOfferSendInternalResult(
        TradeOfferId: tradeOfferId,
        NeedsMobileConfirmation: ParseBoolJson(root, "needs_mobile_confirmation"),
        NeedsEmailConfirmation: ParseBoolJson(root, "needs_email_confirmation")
    );
}

static async Task<bool> ConfirmTradeOfferAsync(CachedSession session, string tradeOfferId)
{
    var confirmations = await FetchConfirmationsAsync(session).ConfigureAwait(false);
    foreach (var conf in confirmations)
    {
        var detailsHtml = await FetchConfirmationDetailsHtmlAsync(session, conf.Id).ConfigureAwait(false);
        var match = Regex.Match(detailsHtml, @"tradeofferid_(\d+)", RegexOptions.IgnoreCase);
        if (!match.Success)
        {
            continue;
        }

        if (!string.Equals(match.Groups[1].Value, tradeOfferId, StringComparison.Ordinal))
        {
            continue;
        }

        if (await ExecuteConfirmationAsync(session, conf, "allow", "allow").ConfigureAwait(false))
        {
            return true;
        }

        return await ExecuteConfirmationAsync(session, conf, "allow", "accept").ConfigureAwait(false);
    }

    return false;
}

static async Task CancelTradeOfferAsync(CachedSession session, string tradeOfferId)
{
    var sessionId = Convert.ToHexString(RandomNumberGenerator.GetBytes(12)).ToLowerInvariant();
    using var http = CreateProxiedHttpClient(session.Proxy, TimeSpan.FromSeconds(30));
    using var request = new HttpRequestMessage(HttpMethod.Post, $"https://steamcommunity.com/tradeoffer/{tradeOfferId}/cancel");
    request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
    {
        ["sessionid"] = sessionId,
    });

    request.Headers.TryAddWithoutValidation("Cookie", BuildSteamCookieHeader(session.SteamId64, session.SteamWebCookie, sessionId));
    request.Headers.TryAddWithoutValidation("Referer", $"https://steamcommunity.com/tradeoffer/{tradeOfferId}/");
    request.Headers.TryAddWithoutValidation("Origin", "https://steamcommunity.com");
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("Accept", "application/json, text/javascript, */*; q=0.01");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"tradeoffer cancel failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}; body={(body.Length > 700 ? body[..700] : body)}");
    }

    if (!string.IsNullOrWhiteSpace(body))
    {
        try
        {
            using var doc = JsonDocument.Parse(body);
            var error = TryGetString(doc.RootElement, "strError");
            if (!string.IsNullOrWhiteSpace(error))
            {
                throw new InvalidOperationException(error);
            }
        }
        catch (JsonException)
        {
            // some successful cancel responses are not json; HTTP 200 is enough
        }
    }
}

static async Task<TradeOfferStatusInternalResponse> GetTradeOfferStatusAsync(CachedSession session, string tradeOfferId)
{
    foreach (var authParam in new[] { "access_token", "key" })
    {
        using var http = CreateProxiedHttpClient(session.Proxy, TimeSpan.FromSeconds(30));
        var url = $"https://api.steampowered.com/IEconService/GetTradeOffer/v1/?{authParam}={Uri.EscapeDataString(session.MarketWebApiToken)}&tradeofferid={Uri.EscapeDataString(tradeOfferId)}&get_descriptions=0";
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
        request.Headers.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");

        using var response = await http.SendAsync(request).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            continue;
        }

        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("response", out var responseElement) || responseElement.ValueKind != JsonValueKind.Object)
        {
            continue;
        }

        if (!responseElement.TryGetProperty("offer", out var offerElement) || offerElement.ValueKind != JsonValueKind.Object)
        {
            continue;
        }

        return new TradeOfferStatusInternalResponse(
            TradeOfferState: ParseIntSafe(TryGetString(offerElement, "trade_offer_state")),
            ConfirmationMethod: ParseIntSafe(TryGetString(offerElement, "confirmation_method")),
            TimeCreated: ParseLongSafe(TryGetString(offerElement, "time_created")),
            TimeUpdated: ParseLongSafe(TryGetString(offerElement, "time_updated")),
            ExpirationTime: ParseLongSafe(TryGetString(offerElement, "expiration_time"))
        );
    }

    throw new InvalidOperationException("unable to fetch trade offer status via Steam WebAPI");
}

static async Task<List<MobileConfirmationItem>> FetchConfirmationsAsync(CachedSession session)
{
    var confirmations = new List<MobileConfirmationItem>();
    using var http = CreateProxiedHttpClient(session.Proxy, TimeSpan.FromSeconds(30));
    var url = BuildConfirmationUrl(session, "getlist", "conf");
    using var request = new HttpRequestMessage(HttpMethod.Get, url);
    request.Headers.TryAddWithoutValidation("Cookie", BuildSteamCookieHeader(session.SteamId64, session.SteamWebCookie));
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("X-Requested-With", "com.valvesoftware.android.steam.community");
    request.Headers.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"mobileconf getlist failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}; body={(body.Length > 500 ? body[..500] : body)}");
    }

    using var doc = JsonDocument.Parse(body);
    var root = doc.RootElement;
    if (!ParseBoolJson(root, "success"))
    {
        var message = TryGetString(root, "message") ?? body;
        throw new InvalidOperationException($"mobileconf getlist failed: {message}");
    }

    if (!root.TryGetProperty("conf", out var confElement) || confElement.ValueKind != JsonValueKind.Array)
    {
        return confirmations;
    }

    foreach (var item in confElement.EnumerateArray())
    {
        var id = TryGetString(item, "id");
        var nonce = TryGetString(item, "nonce");
        if (string.IsNullOrWhiteSpace(id) || string.IsNullOrWhiteSpace(nonce))
        {
            continue;
        }

        confirmations.Add(new MobileConfirmationItem(id, nonce));
    }

    return confirmations;
}

static async Task<string> FetchConfirmationDetailsHtmlAsync(CachedSession session, string confirmationId)
{
    using var http = CreateProxiedHttpClient(session.Proxy, TimeSpan.FromSeconds(30));
    foreach (var tag in new[] { $"details{confirmationId}", "details" })
    {
        var url = BuildConfirmationUrl(session, $"details/{confirmationId}", tag);
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.TryAddWithoutValidation("Cookie", BuildSteamCookieHeader(session.SteamId64, session.SteamWebCookie));
        request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
        request.Headers.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");

        using var response = await http.SendAsync(request).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            continue;
        }

        using var doc = JsonDocument.Parse(body);
        if (!ParseBoolJson(doc.RootElement, "success"))
        {
            continue;
        }

        var html = TryGetString(doc.RootElement, "html");
        if (!string.IsNullOrWhiteSpace(html))
        {
            return html;
        }
    }

    throw new InvalidOperationException($"confirmation details not found for {confirmationId}");
}

static async Task<bool> ExecuteConfirmationAsync(CachedSession session, MobileConfirmationItem confirmation, string op, string tag)
{
    using var http = CreateProxiedHttpClient(session.Proxy, TimeSpan.FromSeconds(30));
    var url = BuildConfirmationUrl(session, "ajaxop", tag, new Dictionary<string, string>
    {
        ["op"] = op,
        ["cid"] = confirmation.Id,
        ["ck"] = confirmation.Nonce,
    });

    using var request = new HttpRequestMessage(HttpMethod.Get, url);
    request.Headers.TryAddWithoutValidation("Cookie", BuildSteamCookieHeader(session.SteamId64, session.SteamWebCookie));
    request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
    request.Headers.TryAddWithoutValidation("X-Requested-With", "XMLHttpRequest");
    request.Headers.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");

    using var response = await http.SendAsync(request).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
    if (!response.IsSuccessStatusCode)
    {
        return false;
    }

    using var doc = JsonDocument.Parse(body);
    return ParseBoolJson(doc.RootElement, "success");
}

static string BuildConfirmationUrl(CachedSession session, string action, string tag, Dictionary<string, string>? extra = null)
{
    var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var query = new List<string>
    {
        $"p={Uri.EscapeDataString(string.IsNullOrWhiteSpace(session.DeviceId) ? GenerateDeviceId(session.SteamId64) : session.DeviceId)}",
        $"a={Uri.EscapeDataString(session.SteamId64)}",
        $"k={GenerateConfirmationKey(session.IdentitySecret, tag, now)}",
        $"t={now}",
        "m=android",
        $"tag={Uri.EscapeDataString(tag)}",
    };

    if (extra is not null)
    {
        foreach (var kv in extra)
        {
            query.Add($"{Uri.EscapeDataString(kv.Key)}={Uri.EscapeDataString(kv.Value)}");
        }
    }

    return $"https://steamcommunity.com/mobileconf/{action}?{string.Join("&", query)}";
}

static MaFileSecrets ResolveMaFile(AccessTokenRequest request)
{
    if (request.MafileData.HasValue && request.MafileData.Value.ValueKind is not JsonValueKind.Null and not JsonValueKind.Undefined)
    {
        return ExtractMaCore(request.MafileData.Value);
    }

    var path = (request.MafilePath ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(path))
    {
        throw new InvalidOperationException("mafile_path or mafile_data is required");
    }

    var raw = File.ReadAllText(path, Encoding.UTF8);
    using var doc = JsonDocument.Parse(raw);
    return ExtractMaCore(doc.RootElement);
}

static MaFileSecrets ExtractMaCore(JsonElement root)
{
    var steamId64 = TryGetNestedString(root, "Session", "SteamID")
                    ?? TryGetString(root, "steamid")
                    ?? TryGetString(root, "SteamID");

    var sharedSecret = TryGetString(root, "shared_secret");
    var identitySecret = TryGetString(root, "identity_secret");
    var deviceId = TryGetString(root, "device_id");

    if (string.IsNullOrWhiteSpace(steamId64))
    {
        throw new InvalidOperationException("steamid64 not found in mafile");
    }

    if (string.IsNullOrWhiteSpace(sharedSecret))
    {
        throw new InvalidOperationException("shared_secret not found in mafile");
    }

    if (string.IsNullOrWhiteSpace(identitySecret))
    {
        throw new InvalidOperationException("identity_secret not found in mafile");
    }

    return new MaFileSecrets(
        SteamId64: steamId64.Trim(),
        SharedSecret: sharedSecret.Trim(),
        IdentitySecret: identitySecret.Trim(),
        DeviceId: deviceId?.Trim() ?? string.Empty
    );
}

static string? TryGetString(JsonElement element, string propertyName)
{
    if (!element.TryGetProperty(propertyName, out var value))
    {
        return null;
    }

    return value.ValueKind switch
    {
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number => value.GetRawText(),
        JsonValueKind.True => bool.TrueString,
        JsonValueKind.False => bool.FalseString,
        _ => value.GetRawText(),
    };
}

static string? TryGetNestedString(JsonElement element, string parentProperty, string childProperty)
{
    if (!element.TryGetProperty(parentProperty, out var parent) || parent.ValueKind != JsonValueKind.Object)
    {
        return null;
    }

    return TryGetString(parent, childProperty);
}

static int GetIntEnv(string key, int defaultValue)
{
    var raw = Environment.GetEnvironmentVariable(key);
    return int.TryParse(raw, out var value) ? value : defaultValue;
}

static void Log(string eventName, object payload)
{
    Console.WriteLine($"{DateTimeOffset.UtcNow:O} [steam_bridge] {eventName} {JsonSerializer.Serialize(payload)}");
}

static string GenerateSteamGuardCode(string sharedSecret)
{
    var secret = Convert.FromBase64String(sharedSecret);
    var timeSlice = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30;

    Span<byte> timeBytes = stackalloc byte[8];
    BinaryPrimitives.WriteInt64BigEndian(timeBytes, timeSlice);

    using var hmac = new HMACSHA1(secret);
    var hash = hmac.ComputeHash(timeBytes.ToArray());

    var offset = hash[^1] & 0x0F;
    var codePoint = ((hash[offset] & 0x7F) << 24)
                  | ((hash[offset + 1] & 0xFF) << 16)
                  | ((hash[offset + 2] & 0xFF) << 8)
                  | (hash[offset + 3] & 0xFF);

    const string chars = "23456789BCDFGHJKMNPQRTVWXY";
    Span<char> code = stackalloc char[5];

    for (var i = 0; i < code.Length; i++)
    {
        code[i] = chars[codePoint % chars.Length];
        codePoint /= chars.Length;
    }

    return new string(code);
}

internal sealed class SharedSecretAuthenticator(string sharedSecret) : IAuthenticator
{
    public Task<string> GetDeviceCodeAsync(bool previousCodeWasIncorrect)
    {
        var code = GenerateSteamGuardCodeInternal(sharedSecret);
        return Task.FromResult(code);
    }

    public Task<string> GetEmailCodeAsync(string email, bool previousCodeWasIncorrect)
    {
        throw new NotSupportedException($"Email Steam Guard is not supported in this bridge. Account email hint: {email}");
    }


    private static string GenerateSteamGuardCodeInternal(string secretBase64)
    {
        var secret = Convert.FromBase64String(secretBase64);
        var timeSlice = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30;

        Span<byte> timeBytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(timeBytes, timeSlice);

        using var hmac = new HMACSHA1(secret);
        var hash = hmac.ComputeHash(timeBytes.ToArray());

        var offset = hash[^1] & 0x0F;
        var codePoint = ((hash[offset] & 0x7F) << 24)
                      | ((hash[offset + 1] & 0xFF) << 16)
                      | ((hash[offset + 2] & 0xFF) << 8)
                      | (hash[offset + 3] & 0xFF);

        const string chars = "23456789BCDFGHJKMNPQRTVWXY";
        Span<char> code = stackalloc char[5];

        for (var i = 0; i < code.Length; i++)
        {
            code[i] = chars[codePoint % chars.Length];
            codePoint /= chars.Length;
        }

        return new string(code);
    }
    public Task<bool> AcceptDeviceConfirmationAsync()
    {
        return Task.FromResult(false);
    }
}

internal sealed record CachedSession(
    string ProfileName,
    string Proxy,
    string Fingerprint,
    string SteamId64,
    string SteamWebCookie,
    string MarketWebApiToken,
    string RefreshToken,
    string IdentitySecret,
    string DeviceId,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset WebCookieExpiresAtUtc,
    DateTimeOffset MarketWebApiTokenExpiresAtUtc,
    DateTimeOffset RefreshTokenExpiresAtUtc,
    string MarketWebApiTokenSource = "steamcommunity_webapi"
);

internal sealed record PersistedSession(
    string ProfileName,
    string Proxy,
    string Fingerprint,
    string SteamId64,
    string SteamWebCookie,
    string MarketWebApiToken,
    string RefreshToken = "",
    string IdentitySecret = "",
    string DeviceId = "",
    DateTimeOffset CreatedAtUtc = default,
    DateTimeOffset WebCookieExpiresAtUtc = default,
    DateTimeOffset MarketWebApiTokenExpiresAtUtc = default,
    DateTimeOffset RefreshTokenExpiresAtUtc = default,
    string MarketWebApiTokenSource = "steamcommunity_webapi"
);

internal sealed record GeneratedAccessTokenResult(
    string AccessToken,
    string RefreshToken,
    DateTimeOffset AccessTokenExpiresAtUtc,
    DateTimeOffset RefreshTokenExpiresAtUtc
);

internal sealed record MaFileSecrets(
    string SteamId64,
    string SharedSecret,
    string IdentitySecret,
    string DeviceId
);

internal class AccessTokenRequest
{
    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("proxy")]
    public string? Proxy { get; set; }

    [JsonPropertyName("steam_username")]
    public string? SteamUsername { get; set; }

    [JsonPropertyName("steam_password")]
    public string? SteamPassword { get; set; }

    [JsonPropertyName("mafile_path")]
    public string? MafilePath { get; set; }

    [JsonPropertyName("mafile_data")]
    public JsonElement? MafileData { get; set; }

    [JsonPropertyName("force_new_session")]
    public bool ForceNewSession { get; set; }
}

internal sealed class InvalidateRequest
{
    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }
}

internal sealed class AccessTokenResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }

    [JsonPropertyName("steamid64")]
    public string? SteamId64 { get; set; }

    [JsonPropertyName("access_token")]
    public string? AccessToken { get; set; }

    [JsonPropertyName("token_source")]
    public string? TokenSource { get; set; }
}

internal sealed record SteamInventoryDescription(
    string MarketHashName,
    string Name,
    int Tradable,
    int Marketable
);

internal sealed record SteamInventoryPageResult(
    List<SteamInventoryItem> Items,
    bool MoreItems,
    string? LastAssetId
);

internal sealed class SteamInventoryRequest : AccessTokenRequest
{
    [JsonPropertyName("count")]
    public int Count { get; set; } = 2000;
}

internal sealed class SteamInventoryResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }

    [JsonPropertyName("steamid64")]
    public string? SteamId64 { get; set; }

    [JsonPropertyName("count")]
    public int Count { get; set; }

    [JsonPropertyName("items")]
    public List<SteamInventoryItem> Items { get; set; } = new();
}

internal sealed class SteamInventoryItem
{
    [JsonPropertyName("assetid")]
    public string? AssetId { get; set; }

    [JsonPropertyName("classid")]
    public string? ClassId { get; set; }

    [JsonPropertyName("instanceid")]
    public string? InstanceId { get; set; }

    [JsonPropertyName("contextid")]
    public string? ContextId { get; set; }

    [JsonPropertyName("amount")]
    public int Amount { get; set; }

    [JsonPropertyName("market_hash_name")]
    public string? MarketHashName { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("tradable")]
    public int Tradable { get; set; }

    [JsonPropertyName("marketable")]
    public int Marketable { get; set; }
}

internal sealed class TradeOfferSendRequest : AccessTokenRequest
{
    [JsonPropertyName("offer")]
    public JsonElement? Offer { get; set; }
}

internal sealed class TradeOfferActionRequest : AccessTokenRequest
{
    [JsonPropertyName("tradeoffer_id")]
    public string? TradeOfferId { get; set; }
}

internal sealed class TradeOfferSendResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }

    [JsonPropertyName("tradeofferid")]
    public string? TradeOfferId { get; set; }

    [JsonPropertyName("needs_mobile_confirmation")]
    public bool NeedsMobileConfirmation { get; set; }

    [JsonPropertyName("needs_email_confirmation")]
    public bool NeedsEmailConfirmation { get; set; }
}

internal sealed class TradeOfferActionResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }

    [JsonPropertyName("tradeoffer_id")]
    public string? TradeOfferId { get; set; }

    [JsonPropertyName("action")]
    public string? Action { get; set; }

    [JsonPropertyName("error")]
    public string? Error { get; set; }
}

internal sealed class TradeOfferStatusResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }

    [JsonPropertyName("tradeoffer_id")]
    public string? TradeOfferId { get; set; }

    [JsonPropertyName("trade_offer_state")]
    public int TradeOfferState { get; set; }

    [JsonPropertyName("confirmation_method")]
    public int ConfirmationMethod { get; set; }

    [JsonPropertyName("time_created")]
    public long TimeCreated { get; set; }

    [JsonPropertyName("time_updated")]
    public long TimeUpdated { get; set; }

    [JsonPropertyName("expiration_time")]
    public long ExpirationTime { get; set; }
}

internal sealed record TradeOfferSendInternalResult(
    string TradeOfferId,
    bool NeedsMobileConfirmation,
    bool NeedsEmailConfirmation
);

internal sealed record TradeOfferStatusInternalResponse(
    int TradeOfferState,
    int ConfirmationMethod,
    long TimeCreated,
    long TimeUpdated,
    long ExpirationTime
);

internal sealed record MobileConfirmationItem(string Id, string Nonce);

internal sealed class ErrorResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("error")]
    public string? Error { get; set; }
}

internal sealed class InvalidateResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("profile_name")]
    public string? ProfileName { get; set; }
}

internal sealed class HealthResponse
{
    [JsonPropertyName("ok")]
    public bool Ok { get; set; }

    [JsonPropertyName("sessions")]
    public int Sessions { get; set; }
}
