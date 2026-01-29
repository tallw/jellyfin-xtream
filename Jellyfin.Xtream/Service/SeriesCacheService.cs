// Copyright (C) 2022  Kevin Jilissen

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Xtream.Client.Models;
using MediaBrowser.Controller.Channels;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Channels;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Providers;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

// Type aliases to disambiguate from MediaBrowser.Controller.Entities.TV types
using JellyfinSeries = MediaBrowser.Controller.Entities.TV.Series;
using JellyfinSeriesInfo = MediaBrowser.Controller.Providers.SeriesInfo;
using XtreamEpisode = Jellyfin.Xtream.Client.Models.Episode;
using XtreamSeason = Jellyfin.Xtream.Client.Models.Season;
using XtreamSeries = Jellyfin.Xtream.Client.Models.Series;
using XtreamSeriesInfo = Jellyfin.Xtream.Client.Models.SeriesInfo;

namespace Jellyfin.Xtream.Service;

/// <summary>
/// Service for pre-fetching and caching all series data upfront.
/// </summary>
public class SeriesCacheService : IDisposable
{
    private readonly StreamService _streamService;
    private readonly IMemoryCache _memoryCache;
    private readonly FailureTrackingService _failureTrackingService;
    private readonly ILogger<SeriesCacheService>? _logger;
    private readonly IProviderManager? _providerManager;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private int _cacheVersion = 0;
    private bool _isRefreshing = false;
    private double _currentProgress = 0.0;
    private string _currentStatus = "Idle";
    private DateTime? _lastRefreshStart;
    private DateTime? _lastRefreshComplete;
    private CancellationTokenSource? _refreshCancellationTokenSource;

    /// <summary>
    /// Initializes a new instance of the <see cref="SeriesCacheService"/> class.
    /// </summary>
    /// <param name="streamService">The stream service instance.</param>
    /// <param name="memoryCache">The memory cache instance.</param>
    /// <param name="failureTrackingService">The failure tracking service instance.</param>
    /// <param name="logger">Optional logger instance.</param>
    /// <param name="providerManager">Optional provider manager for TMDB lookups.</param>
    public SeriesCacheService(
        StreamService streamService,
        IMemoryCache memoryCache,
        FailureTrackingService failureTrackingService,
        ILogger<SeriesCacheService>? logger = null,
        IProviderManager? providerManager = null)
    {
        _streamService = streamService;
        _memoryCache = memoryCache;
        _failureTrackingService = failureTrackingService;
        _logger = logger;
        _providerManager = providerManager;
    }

    /// <summary>
    /// Gets the current cache key prefix.
    /// Uses CacheDataVersion which only changes when cache-relevant settings change
    /// (not when refresh frequency changes).
    /// </summary>
    private string CachePrefix => $"series_cache_{Plugin.Instance.CacheDataVersion}_v{_cacheVersion}_";

    /// <summary>
    /// Pre-fetches and caches all series data (categories, series, seasons, episodes).
    /// </summary>
    /// <param name="progress">Optional progress reporter (0.0 to 1.0).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the async operation.</returns>
    public async Task RefreshCacheAsync(IProgress<double>? progress = null, CancellationToken cancellationToken = default)
    {
        // Prevent concurrent refreshes
        if (!await _refreshLock.WaitAsync(0, cancellationToken).ConfigureAwait(false))
        {
            _logger?.LogInformation("Cache refresh already in progress, skipping");
            return;
        }

        try
        {
            if (_isRefreshing)
            {
                _logger?.LogInformation("Cache refresh already in progress, skipping");
                return;
            }

            _isRefreshing = true;
            _currentProgress = 0.0;
            _currentStatus = "Starting...";
            _lastRefreshStart = DateTime.UtcNow;

            // Create a linked cancellation token source so we can cancel the refresh
            _refreshCancellationTokenSource?.Dispose();
            _refreshCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _logger?.LogInformation("Starting series data cache refresh");

            string cacheDataVersion = Plugin.Instance.CacheDataVersion;
            string cachePrefix = $"series_cache_{cacheDataVersion}_v{_cacheVersion}_";

            // Clear old cache entries
            ClearCache(cacheDataVersion);

            try
            {
                // Cache entries have a 24-hour safety expiration to prevent memory leaks
                // from orphaned entries (e.g., when cache version changes).
                // Normal refresh frequency is controlled by the scheduled task (default: every 60 minutes)
                MemoryCacheEntryOptions cacheOptions = new()
                {
                    AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(24)
                };

                // Fetch all categories
                _currentStatus = "Fetching categories...";
                progress?.Report(0.05);
                _logger?.LogInformation("Fetching series categories...");
                IEnumerable<Category> categories = await _streamService.GetSeriesCategories(_refreshCancellationTokenSource.Token).ConfigureAwait(false);
                List<Category> categoryList = categories.ToList();
                _memoryCache.Set($"{cachePrefix}categories", categoryList, cacheOptions);
                _logger?.LogInformation("Found {CategoryCount} categories", categoryList.Count);

                // Log configuration state for debugging
                var seriesConfig = Plugin.Instance.Configuration.Series;
                _logger?.LogInformation("Configuration has {ConfigCategoryCount} configured series categories", seriesConfig.Count);
                foreach (var kvp in seriesConfig)
                {
                    if (kvp.Value.Count == 0)
                    {
                        _logger?.LogInformation("  Category {CategoryId}: ALL series allowed (empty config)", kvp.Key);
                    }
                    else
                    {
                        _logger?.LogInformation("  Category {CategoryId}: {SeriesCount} specific series configured", kvp.Key, kvp.Value.Count);
                    }
                }

                int seriesCount = 0;
                int seasonCount = 0;
                int episodeCount = 0;
                int totalSeries = 0;

                // Single pass: fetch all series lists and cache them for reuse
                // This eliminates the double API call that was happening before
                Dictionary<int, List<XtreamSeries>> seriesListsByCategory = new();
                _currentStatus = "Fetching series lists...";
                foreach (Category category in categoryList)
                {
                    _refreshCancellationTokenSource.Token.ThrowIfCancellationRequested();
                    IEnumerable<XtreamSeries> seriesList = await _streamService.GetSeries(category.CategoryId, _refreshCancellationTokenSource.Token).ConfigureAwait(false);
                    List<XtreamSeries> seriesItems = seriesList.ToList();
                    seriesListsByCategory[category.CategoryId] = seriesItems;
                    totalSeries += seriesItems.Count;
                }

                _logger?.LogInformation("Fetched {TotalSeries} series across {CategoryCount} categories", totalSeries, categoryList.Count);

                // Get parallelism configuration
                int parallelism = Math.Max(1, Math.Min(10, Plugin.Instance?.Configuration.CacheRefreshParallelism ?? 3));
                int minDelayMs = Math.Max(0, Math.Min(1000, Plugin.Instance?.Configuration.CacheRefreshMinDelayMs ?? 100));
                _logger?.LogInformation("Starting parallel series processing with parallelism={Parallelism}, minDelayMs={MinDelayMs}", parallelism, minDelayMs);

                // Throttle semaphore for rate limiting API requests
                using SemaphoreSlim throttleSemaphore = new(1, 1);
                DateTime lastRequestTime = DateTime.MinValue;

                // Helper to throttle requests
                async Task ThrottleRequestAsync()
                {
                    if (minDelayMs <= 0)
                    {
                        return;
                    }

                    await throttleSemaphore.WaitAsync(_refreshCancellationTokenSource.Token).ConfigureAwait(false);
                    try
                    {
                        double elapsedMs = (DateTime.UtcNow - lastRequestTime).TotalMilliseconds;
                        if (elapsedMs < minDelayMs)
                        {
                            await Task.Delay(minDelayMs - (int)elapsedMs, _refreshCancellationTokenSource.Token).ConfigureAwait(false);
                        }

                        lastRequestTime = DateTime.UtcNow;
                    }
                    finally
                    {
                        throttleSemaphore.Release();
                    }
                }

                // Flatten all series into a single list with category info for parallel processing
                List<(XtreamSeries Series, Category Category)> allSeries = new();
                foreach (Category category in categoryList)
                {
                    List<XtreamSeries> seriesListItems = seriesListsByCategory[category.CategoryId];
                    _memoryCache.Set($"{cachePrefix}serieslist_{category.CategoryId}", seriesListItems, cacheOptions);
                    foreach (XtreamSeries series in seriesListItems)
                    {
                        allSeries.Add((series, category));
                    }
                }

                // Thread-safe counters for progress tracking
                int processedSeries = 0;

                // Parallel processing options
                ParallelOptions parallelOptions = new()
                {
                    MaxDegreeOfParallelism = parallelism,
                    CancellationToken = _refreshCancellationTokenSource.Token
                };

                await Parallel.ForEachAsync(allSeries, parallelOptions, async (item, ct) =>
                {
                    XtreamSeries series = item.Series;

                    try
                    {
                        // Throttle to prevent rate limiting
                        await ThrottleRequestAsync().ConfigureAwait(false);

                        // Fetch seasons for this series (makes ONE API call to get SeriesStreamInfo)
                        IEnumerable<Tuple<SeriesStreamInfo, int>> seasons = await _streamService.GetSeasons(series.SeriesId, ct).ConfigureAwait(false);
                        List<Tuple<SeriesStreamInfo, int>> seasonList = seasons.ToList();

                        // Reuse the SeriesStreamInfo from GetSeasons for all episodes
                        // This eliminates redundant API calls (was calling GetSeriesStreamsBySeriesAsync once per season)
                        SeriesStreamInfo? seriesStreamInfo = seasonList.FirstOrDefault()?.Item1;

                        int localSeasonCount = 0;
                        int localEpisodeCount = 0;

                        foreach (var seasonTuple in seasonList)
                        {
                            int seasonId = seasonTuple.Item2;
                            localSeasonCount++;

                            // Get episodes from the already-fetched SeriesStreamInfo (no API call)
                            IEnumerable<Tuple<SeriesStreamInfo, XtreamSeason?, XtreamEpisode>> episodes = _streamService.GetEpisodesFromSeriesInfo(seriesStreamInfo!, series.SeriesId, seasonId);

                            List<XtreamEpisode> episodeList = episodes.Select(e => e.Item3).ToList();
                            localEpisodeCount += episodeList.Count;

                            // Cache episodes for this season
                            _memoryCache.Set($"{cachePrefix}episodes_{series.SeriesId}_{seasonId}", episodeList, cacheOptions);

                            // Cache season info
                            XtreamSeason? season = seriesStreamInfo?.Seasons.FirstOrDefault(s => s.SeasonId == seasonId);
                            _memoryCache.Set($"{cachePrefix}season_{series.SeriesId}_{seasonId}", season, cacheOptions);
                        }

                        // Cache series stream info
                        if (seriesStreamInfo != null)
                        {
                            _memoryCache.Set($"{cachePrefix}seriesinfo_{series.SeriesId}", seriesStreamInfo, cacheOptions);
                        }

                        // Update counters atomically
                        int currentProcessed = Interlocked.Increment(ref processedSeries);
                        Interlocked.Add(ref seriesCount, 1);
                        Interlocked.Add(ref seasonCount, localSeasonCount);
                        Interlocked.Add(ref episodeCount, localEpisodeCount);

                        // Update progress (thread-safe since only read by UI)
                        if (totalSeries > 0)
                        {
                            double progressValue = 0.1 + (currentProcessed * 0.8 / totalSeries);
                            _currentProgress = progressValue;
                            _currentStatus = $"Processing series {currentProcessed}/{totalSeries} ({seriesCount} series, {seasonCount} seasons, {episodeCount} episodes)";
                            progress?.Report(progressValue);
                        }

                        // Log progress every 50 series
                        if (currentProcessed % 50 == 0)
                        {
                            _logger?.LogInformation(
                                "Progress: {Processed}/{Total} series ({Seasons} seasons, {Episodes} episodes)",
                                currentProcessed,
                                totalSeries,
                                seasonCount,
                                episodeCount);
                        }
                    }
                    catch (HttpRequestException ex) when (ex.StatusCode >= HttpStatusCode.InternalServerError)
                    {
                        // HTTP 5xx errors - already retried by RetryHandler if enabled
                        _logger?.LogWarning(
                            "Persistent HTTP {StatusCode} error for series {SeriesId} ({SeriesName}) after {MaxRetries} retries: {Message}",
                            ex.StatusCode,
                            series.SeriesId,
                            series.Name,
                            Plugin.Instance?.Configuration.HttpRetryMaxAttempts ?? 3,
                            ex.Message);
                        Interlocked.Increment(ref processedSeries);
                    }
                    catch (OperationCanceledException)
                    {
                        throw; // Let cancellation propagate
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning(ex, "Failed to cache data for series {SeriesId} ({SeriesName})", series.SeriesId, series.Name);
                        Interlocked.Increment(ref processedSeries);
                    }
                }).ConfigureAwait(false);

                _logger?.LogInformation(
                    "Parallel processing completed: {SeriesCount} series, {SeasonCount} seasons, {EpisodeCount} episodes",
                    seriesCount,
                    seasonCount,
                    episodeCount);

                // Fetch TVDb images for series if enabled
                bool useTvdb = Plugin.Instance?.Configuration.UseTvdbForSeriesMetadata ?? true;
                if (useTvdb && _providerManager != null)
                {
                    _logger?.LogInformation("Looking up TVDb metadata for {Count} series...", totalSeries);
                    _currentStatus = "Fetching TVDb images...";

                    // Parse title overrides once before the lookup loop
                    Dictionary<string, string> titleOverrides = ParseTitleOverrides(
                        Plugin.Instance?.Configuration.TvdbTitleOverrides ?? string.Empty);

                    if (titleOverrides.Count > 0)
                    {
                        _logger?.LogInformation("Loaded {Count} TVDb title overrides", titleOverrides.Count);
                    }

                    int tmdbFound = 0;
                    int tmdbNotFound = 0;

                    foreach (var kvp in seriesListsByCategory)
                    {
                        foreach (var series in kvp.Value)
                        {
                            _refreshCancellationTokenSource.Token.ThrowIfCancellationRequested();

                            string? tmdbUrl = await LookupAndCacheTmdbImageAsync(
                                series.SeriesId,
                                series.Name,
                                titleOverrides,
                                cacheOptions,
                                _refreshCancellationTokenSource.Token).ConfigureAwait(false);

                            if (tmdbUrl != null)
                            {
                                tmdbFound++;
                            }
                            else
                            {
                                tmdbNotFound++;
                            }
                        }
                    }

                    _logger?.LogInformation(
                        "TVDb lookup completed: {Found} found, {NotFound} not found",
                        tmdbFound,
                        tmdbNotFound);
                }

                progress?.Report(1.0); // 100% complete
                _currentProgress = 1.0;
                _currentStatus = $"Completed: {seriesCount} series, {seasonCount} seasons, {episodeCount} episodes";
                _lastRefreshComplete = DateTime.UtcNow;
                _logger?.LogInformation("Cache refresh completed: {SeriesCount} series, {SeasonCount} seasons, {EpisodeCount} episodes across {CategoryCount} categories", seriesCount, seasonCount, episodeCount, categoryList.Count);

                // Log failure summary if failures occurred
                var (failureCount, failedItems) = _failureTrackingService.GetFailureStats();
                if (failureCount > 0)
                {
                    _logger?.LogWarning(
                        "Cache refresh completed with {FailureCount} persistent HTTP failures. " +
                        "These items will be skipped for the next {ExpirationHours} hours. " +
                        "First 10 failed URLs: {FailedItems}",
                        failureCount,
                        Plugin.Instance?.Configuration.HttpFailureCacheExpirationHours ?? 24,
                        string.Join(", ", failedItems.Take(10)));
                }

                // Eagerly populate Jellyfin's database by fetching all channel items
                // This ensures browsing is instant without any lazy loading
                try
                {
                    _logger?.LogInformation("Starting eager population of Jellyfin database from cache...");
                    await PopulateJellyfinDatabaseAsync(_refreshCancellationTokenSource.Token).ConfigureAwait(false);
                    _logger?.LogInformation("Jellyfin database population completed");
                }
                catch (OperationCanceledException)
                {
                    _logger?.LogInformation("Database population cancelled");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to populate Jellyfin database - items may load lazily when browsing");
                }
            }
            catch (OperationCanceledException)
            {
                _logger?.LogInformation("Cache refresh cancelled");
                _currentStatus = "Cancelled";
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error during cache refresh");
                throw;
            }
        }
        finally
        {
            _isRefreshing = false;
            if (_currentProgress < 1.0)
            {
                _currentStatus = "Failed or cancelled";
            }

            _refreshLock.Release();
        }
    }

    /// <summary>
    /// Gets cached categories.
    /// </summary>
    /// <returns>Cached categories, or null if not available.</returns>
    public IEnumerable<Category>? GetCachedCategories()
    {
        try
        {
            string cacheKey = $"{CachePrefix}categories";
            if (_memoryCache.TryGetValue(cacheKey, out List<Category>? categories) && categories != null)
            {
                return categories;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Gets cached series stream info.
    /// </summary>
    /// <param name="seriesId">The series ID.</param>
    /// <returns>Cached series stream info, or null if not available.</returns>
    public SeriesStreamInfo? GetCachedSeriesInfo(int seriesId)
    {
        try
        {
            string cacheKey = $"{CachePrefix}seriesinfo_{seriesId}";
            return _memoryCache.TryGetValue(cacheKey, out SeriesStreamInfo? info) ? info : null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Gets cached season info.
    /// </summary>
    /// <param name="seriesId">The series ID.</param>
    /// <param name="seasonId">The season ID.</param>
    /// <returns>Cached season info, or null if not available.</returns>
    public XtreamSeason? GetCachedSeason(int seriesId, int seasonId)
    {
        try
        {
            string cacheKey = $"{CachePrefix}season_{seriesId}_{seasonId}";
            return _memoryCache.TryGetValue(cacheKey, out XtreamSeason? season) ? season : null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Gets cached episodes for a season.
    /// </summary>
    /// <param name="seriesId">The series ID.</param>
    /// <param name="seasonId">The season ID.</param>
    /// <returns>Cached episodes, or null if not available.</returns>
    public IEnumerable<XtreamEpisode>? GetCachedEpisodes(int seriesId, int seasonId)
    {
        try
        {
            string cacheKey = $"{CachePrefix}episodes_{seriesId}_{seasonId}";
            if (_memoryCache.TryGetValue(cacheKey, out List<XtreamEpisode>? episodes) && episodes != null)
            {
                return episodes;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Gets cached series list for a category.
    /// </summary>
    /// <param name="categoryId">The category ID.</param>
    /// <returns>Cached series list, or null if not available.</returns>
    public IEnumerable<XtreamSeries>? GetCachedSeriesList(int categoryId)
    {
        try
        {
            string cacheKey = $"{CachePrefix}serieslist_{categoryId}";
            if (_memoryCache.TryGetValue(cacheKey, out List<XtreamSeries>? seriesList) && seriesList != null)
            {
                return seriesList;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Gets the cached TMDB image URL for a series.
    /// </summary>
    /// <param name="seriesId">The series ID.</param>
    /// <returns>TMDB image URL, or null if not cached.</returns>
    public string? GetCachedTmdbImageUrl(int seriesId)
    {
        try
        {
            string cacheKey = $"{CachePrefix}tmdb_image_{seriesId}";
            if (_memoryCache.TryGetValue(cacheKey, out string? imageUrl) && imageUrl != null)
            {
                return imageUrl;
            }

            _logger?.LogWarning("No cached TVDb image found for series {SeriesId} (key: {CacheKey})", seriesId, cacheKey);
            return null;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Error retrieving cached TVDb image for series {SeriesId}", seriesId);
            return null;
        }
    }

    /// <summary>
    /// Looks up TMDB image URL for a series and caches it.
    /// Checks title overrides first for direct TVDb ID lookup, then falls back to name search.
    /// </summary>
    /// <param name="seriesId">The Xtream series ID.</param>
    /// <param name="seriesName">The series name to search for.</param>
    /// <param name="titleOverrides">Title-to-TVDb-ID override map.</param>
    /// <param name="cacheOptions">Cache options to use.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The TMDB image URL if found, null otherwise.</returns>
    private async Task<string?> LookupAndCacheTmdbImageAsync(
        int seriesId,
        string seriesName,
        Dictionary<string, string> titleOverrides,
        MemoryCacheEntryOptions cacheOptions,
        CancellationToken cancellationToken)
    {
        if (_providerManager == null)
        {
            return null;
        }

        try
        {
            // Parse the name to remove tags
            string cleanName = StreamService.ParseName(seriesName).Title;
            if (string.IsNullOrWhiteSpace(cleanName))
            {
                return null;
            }

            // Check title overrides first for direct TVDb ID lookup
            if (titleOverrides.TryGetValue(cleanName, out string? tvdbId))
            {
                _logger?.LogInformation("Using TVDb title override for series {SeriesId} ({Name}) → TVDb ID {TvdbId}", seriesId, cleanName, tvdbId);
                string? overrideResult = await LookupByTvdbIdAsync(cleanName, tvdbId, cancellationToken).ConfigureAwait(false);
                if (overrideResult != null)
                {
                    string cacheKey = $"{CachePrefix}tmdb_image_{seriesId}";
                    _memoryCache.Set(cacheKey, overrideResult, cacheOptions);
                    _logger?.LogInformation("Cached TVDb image for series {SeriesId} ({Name}) via override (TVDb ID {TvdbId}): {Url}", seriesId, cleanName, tvdbId, overrideResult);
                    return overrideResult;
                }

                _logger?.LogWarning("TVDb title override for series {SeriesId} ({Name}) with TVDb ID {TvdbId} returned no image, falling back to name search", seriesId, cleanName, tvdbId);
            }

            // Fall back to name-based search with progressively cleaned search terms
            string[] searchTerms = GenerateSearchTerms(cleanName);

            foreach (string searchTerm in searchTerms)
            {
                if (string.IsNullOrWhiteSpace(searchTerm))
                {
                    continue;
                }

                // Search TVDb for the series
                RemoteSearchQuery<JellyfinSeriesInfo> query = new()
                {
                    SearchInfo = new() { Name = searchTerm },
                    SearchProviderName = "TheTVDB",
                };

                IEnumerable<RemoteSearchResult> results = await _providerManager
                    .GetRemoteSearchResults<JellyfinSeries, JellyfinSeriesInfo>(query, cancellationToken)
                    .ConfigureAwait(false);

                // Find first result with a real image (not a placeholder)
                RemoteSearchResult? resultWithImage = results.FirstOrDefault(r =>
                    !string.IsNullOrEmpty(r.ImageUrl) &&
                    !r.ImageUrl.Contains("missing/series", StringComparison.OrdinalIgnoreCase) &&
                    !r.ImageUrl.Contains("missing/movie", StringComparison.OrdinalIgnoreCase));
                if (resultWithImage?.ImageUrl != null)
                {
                    // Cache the TVDb image URL
                    string cacheKey = $"{CachePrefix}tmdb_image_{seriesId}";
                    _memoryCache.Set(cacheKey, resultWithImage.ImageUrl, cacheOptions);
                    _logger?.LogInformation("Cached TVDb image for series {SeriesId} ({Name}) using search term '{SearchTerm}': {Url}", seriesId, cleanName, searchTerm, resultWithImage.ImageUrl);
                    return resultWithImage.ImageUrl;
                }
            }

            _logger?.LogWarning("TVDb search found no image for series {SeriesId} ({Name}) after trying: {SearchTerms}", seriesId, cleanName, string.Join(", ", searchTerms));
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to lookup TVDb image for series {SeriesId} ({Name})", seriesId, seriesName);
        }

        return null;
    }

    /// <summary>
    /// Looks up a series on TVDb by its TVDb ID and returns the image URL.
    /// </summary>
    /// <param name="cleanName">The cleaned series name (for logging).</param>
    /// <param name="tvdbId">The TVDb ID to look up.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The image URL if found, null otherwise.</returns>
    private async Task<string?> LookupByTvdbIdAsync(
        string cleanName,
        string tvdbId,
        CancellationToken cancellationToken)
    {
        if (_providerManager == null)
        {
            return null;
        }

        RemoteSearchQuery<JellyfinSeriesInfo> query = new()
        {
            SearchInfo = new()
            {
                Name = cleanName,
                ProviderIds = new Dictionary<string, string>
                {
                    { MetadataProvider.Tvdb.ToString(), tvdbId }
                }
            },
            SearchProviderName = "TheTVDB",
        };

        IEnumerable<RemoteSearchResult> results = await _providerManager
            .GetRemoteSearchResults<JellyfinSeries, JellyfinSeriesInfo>(query, cancellationToken)
            .ConfigureAwait(false);

        RemoteSearchResult? resultWithImage = results.FirstOrDefault(r =>
            !string.IsNullOrEmpty(r.ImageUrl) &&
            !r.ImageUrl.Contains("missing/series", StringComparison.OrdinalIgnoreCase) &&
            !r.ImageUrl.Contains("missing/movie", StringComparison.OrdinalIgnoreCase));

        return resultWithImage?.ImageUrl;
    }

    /// <summary>
    /// Generates search terms from a series name for TVDb lookup.
    /// Returns the original name and a variant with language indicators stripped.
    /// </summary>
    /// <param name="name">The original series name.</param>
    /// <returns>Array of search terms to try.</returns>
    private static string[] GenerateSearchTerms(string name)
    {
        List<string> terms = new();

        // 1. Add original name first
        terms.Add(name);

        // 2. Remove language indicators like "(NL Gesproken)", "(DE)", "(French)", etc.
        string withoutLang = System.Text.RegularExpressions.Regex.Replace(
            name,
            @"\s*\([^)]*(?:Gesproken|Dubbed|Subbed|NL|DE|FR|Dutch|German|French|Nederlands|Deutsch)[^)]*\)\s*",
            string.Empty,
            System.Text.RegularExpressions.RegexOptions.IgnoreCase).Trim();

        if (!string.Equals(withoutLang, name, StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(withoutLang))
        {
            terms.Add(withoutLang);
        }

        return terms.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    /// <summary>
    /// Parses the title override configuration string into a dictionary.
    /// Format: one mapping per line, "SeriesTitle=TVDbID".
    /// </summary>
    private static Dictionary<string, string> ParseTitleOverrides(string config)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(config))
        {
            return result;
        }

        foreach (string line in config.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int equalsIndex = line.IndexOf('=', StringComparison.Ordinal);
            if (equalsIndex > 0 && equalsIndex < line.Length - 1)
            {
                string key = line[..equalsIndex].Trim();
                string value = line[(equalsIndex + 1)..].Trim();
                if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(value))
                {
                    result[key] = value;
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Clears all cache entries for the given data version.
    /// </summary>
    private void ClearCache(string dataVersion)
    {
        // Note: IMemoryCache doesn't support enumerating keys, so we can't clear by prefix
        // Instead, we rely on cache expiration and version-based keys
        // When data version changes, old keys won't be accessed anymore
        _logger?.LogInformation("Cache cleared (old entries will expire naturally)");
    }

    /// <summary>
    /// Checks if cache is populated for the current data version.
    /// </summary>
    /// <returns>True if cache is populated, false otherwise.</returns>
    public bool IsCachePopulated()
    {
        try
        {
            string cacheKey = $"{CachePrefix}categories";
            return _memoryCache.TryGetValue(cacheKey, out _);
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Cancels the currently running cache refresh operation.
    /// </summary>
    public void CancelRefresh()
    {
        if (_isRefreshing && _refreshCancellationTokenSource != null)
        {
            _logger?.LogInformation("Cancelling cache refresh...");
            _currentStatus = "Cancelling...";
            _refreshCancellationTokenSource.Cancel();
        }
    }

    /// <summary>
    /// Invalidates all cached data by incrementing the cache version.
    /// Old cache entries will remain in memory but won't be accessed.
    /// </summary>
    public void InvalidateCache()
    {
        _cacheVersion++;
        _currentProgress = 0.0;
        _currentStatus = "Cache invalidated";
        _lastRefreshComplete = null;
        _logger?.LogInformation("Cache invalidated (version incremented to {Version})", _cacheVersion);
    }

    /// <summary>
    /// Eagerly populates Jellyfin's database by fetching all channel items.
    /// This forces Jellyfin to store all series, seasons, and episodes in jellyfin.db
    /// so browsing is instant without any lazy loading.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the async operation.</returns>
    public async Task PopulateJellyfinDatabaseAsync(CancellationToken cancellationToken)
    {
        _currentStatus = "Populating Jellyfin database...";

        try
        {
            IChannelManager? channelManager = Plugin.Instance?.ChannelManager;
            if (channelManager == null)
            {
                _logger?.LogWarning("ChannelManager not available, skipping eager database population");
                return;
            }

            // Find our Series channel
            _logger?.LogInformation("Looking for Xtream Series channel...");
            var channelQuery = new ChannelQuery();
            var channelsResult = await channelManager.GetChannelsInternalAsync(channelQuery).ConfigureAwait(false);
            _logger?.LogInformation("Found {Count} channels total", channelsResult.TotalRecordCount);

            var seriesChannel = channelsResult.Items.FirstOrDefault(c => c.Name == "Xtream Series");
            if (seriesChannel == null)
            {
                _logger?.LogWarning("Xtream Series channel not found in {Count} channels, skipping eager database population", channelsResult.TotalRecordCount);
                return;
            }

            Guid channelId = seriesChannel.Id;
            _logger?.LogInformation("Found Xtream Series channel with ID {ChannelId}", channelId);

            // Get root level items (all series in flat mode, or categories)
            var rootQuery = new InternalItemsQuery
            {
                ChannelIds = new[] { channelId },
                Recursive = false
            };

            _logger?.LogInformation("Fetching root level channel items to populate database...");

            // Use a simple progress reporter that logs
            var progress = new Progress<double>(p =>
            {
                if (p > 0 && (int)(p * 100) % 10 == 0)
                {
                    _logger?.LogDebug("Root fetch progress: {Progress:P0}", p);
                }
            });

            var rootResult = await channelManager.GetChannelItemsInternal(
                rootQuery,
                progress,
                cancellationToken).ConfigureAwait(false);

            int rootCount = rootResult?.TotalRecordCount ?? 0;
            int itemCount = rootResult?.Items?.Count ?? 0;
            _logger?.LogInformation("Root level fetch complete: TotalRecordCount={TotalCount}, Items.Count={ItemCount}", rootCount, itemCount);

            if (rootResult?.Items == null || itemCount == 0)
            {
                _logger?.LogInformation("No root items to process, database population complete");
                return;
            }

            // Now iterate through each series to fetch seasons
            int seriesProcessed = 0;
            int seasonsProcessed = 0;
            int episodesProcessed = 0;
            int errorCount = 0;

            _logger?.LogInformation("Processing {Count} root items for seasons and episodes...", itemCount);

            foreach (var item in rootResult.Items)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Process all folders (series, categories, etc.)
                if (item is Folder)
                {
                    seriesProcessed++;

                    try
                    {
                        // Get child items (seasons for series, series for categories)
                        var childQuery = new InternalItemsQuery
                        {
                            ChannelIds = new[] { channelId },
                            ParentId = item!.Id,
                            Recursive = false
                        };

                        var childResult = await channelManager.GetChannelItemsInternal(
                            childQuery,
                            new Progress<double>(),
                            cancellationToken).ConfigureAwait(false);

                        // Process grandchildren (episodes for seasons)
                        if (childResult?.Items != null)
                        {
                            foreach (var childItem in childResult.Items)
                            {
                                cancellationToken.ThrowIfCancellationRequested();

                                if (childItem is Folder)
                                {
                                    try
                                    {
                                        seasonsProcessed++;

                                        var grandchildQuery = new InternalItemsQuery
                                        {
                                            ChannelIds = new[] { channelId },
                                            ParentId = childItem.Id,
                                            Recursive = false
                                        };

                                        var grandchildResult = await channelManager.GetChannelItemsInternal(
                                            grandchildQuery,
                                            new Progress<double>(),
                                            cancellationToken).ConfigureAwait(false);

                                        episodesProcessed += grandchildResult?.TotalRecordCount ?? 0;
                                    }
                                    catch (Exception ex) when (ex is not OperationCanceledException)
                                    {
                                        errorCount++;
                                        _logger?.LogDebug(ex, "Error populating episodes for season {SeasonId}", childItem.Id);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        errorCount++;
                        _logger?.LogDebug(ex, "Error populating seasons for item {ItemId}", item.Id);
                    }

                    // Log progress every 5 series (less frequent since we have fewer)
                    if (seriesProcessed % 5 == 0 || seriesProcessed == itemCount)
                    {
                        _logger?.LogInformation(
                            "Database population progress: {Series}/{Total} items, {Seasons} seasons, {Episodes} episodes",
                            seriesProcessed,
                            itemCount,
                            seasonsProcessed,
                            episodesProcessed);
                        _currentStatus = $"Populating database: {seriesProcessed}/{itemCount} items...";
                    }
                }
            }

            if (errorCount > 0)
            {
                _logger?.LogWarning(
                    "Jellyfin database population completed with {ErrorCount} errors: {Series} items, {Seasons} seasons, {Episodes} episodes",
                    errorCount,
                    seriesProcessed,
                    seasonsProcessed,
                    episodesProcessed);
            }
            else
            {
                _logger?.LogInformation(
                    "Jellyfin database population completed: {Series} items, {Seasons} seasons, {Episodes} episodes",
                    seriesProcessed,
                    seasonsProcessed,
                    episodesProcessed);
            }

            _currentStatus = $"Database populated: {seriesProcessed} items, {seasonsProcessed} seasons, {episodesProcessed} episodes";
        }
        catch (OperationCanceledException)
        {
            _logger?.LogInformation("Database population cancelled");
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error during Jellyfin database population");
        }
    }

    /// <summary>
    /// Gets the current cache refresh status.
    /// </summary>
    /// <returns>Cache status information.</returns>
    public (bool IsRefreshing, double Progress, string Status, DateTime? StartTime, DateTime? CompleteTime) GetStatus()
    {
        return (_isRefreshing, _currentProgress, _currentStatus, _lastRefreshStart, _lastRefreshComplete);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases the unmanaged resources used by the SeriesCacheService and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing">True to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _refreshCancellationTokenSource?.Dispose();
            _refreshLock?.Dispose();
        }
    }
}
