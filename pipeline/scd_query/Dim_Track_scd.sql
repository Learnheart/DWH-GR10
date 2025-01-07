-- scd type 2

MERGE `final-dwh.chinook_olap.DIM_Track` T
USING (
    SELECT 
        t.TrackId AS dim_track_id, 
        t.Name AS track_name,
        al.Title AS album_name, 
        ar.Name AS artist_name,
        t.Composer AS composer,
        g.Name AS genre,
        m.Name AS mediatype
    FROM `final-dwh.chinook_stagging.track` t
    JOIN `final-dwh.chinook_stagging.album` al ON t.AlbumId = al.AlbumId
    JOIN `final-dwh.chinook_stagging.artist` ar ON al.ArtistId = ar.ArtistId
    JOIN `final-dwh.chinook_stagging.mediaType` m ON t.MediaTypeId = m.MediaTypeId
    JOIN `final-dwh.chinook_stagging.genre` g ON t.GenreId = g.GenreId
) S
ON T.dim_track_id = S.dim_track_id

-- Update existing records if any data has changed
WHEN MATCHED AND (
    T.track_name != S.track_name OR
    T.album_name != S.album_name OR
    T.artist_name != S.artist_name OR
    T.composer != S.composer OR
    T.genre != S.genre OR
    T.mediatype != S.mediatype
) THEN
    UPDATE SET 
        track_name = S.track_name,
        album_name = S.album_name,
        artist_name = S.artist_name,
        composer = S.composer,
        genre = S.genre,
        mediatype = S.mediatype

-- Insert new records
WHEN NOT MATCHED THEN
    INSERT (dim_track_id, track_name, album_name, artist_name, composer, genre, mediatype)
    VALUES (S.dim_track_id, S.track_name, S.album_name, S.artist_name, S.composer, S.genre, S.mediatype)

-- delete
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;