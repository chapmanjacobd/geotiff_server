from terracotta import update_settings


update_settings(
    DRIVER_PATH="db.sqlite",
    DRIVER_PROVIDER="sqlite",
    REPROJECTION_METHOD="nearest",
    RASTER_CACHE_SIZE=1000000000,
    RASTER_CACHE_COMPRESS_LEVEL=1,
    PNG_COMPRESS_LEVEL=2,
    DEFAULT_TILE_SIZE=[512, 512],
    ALLOWED_ORIGINS_METADATA='["*"]',
    ALLOWED_ORIGINS_TILES='["*"]',
    LOGLEVEL="info"
    # DEBUG=True,
)
