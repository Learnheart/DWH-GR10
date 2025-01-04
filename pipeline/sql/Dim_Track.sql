CREATE OR REPLACE TABLE `final-dwh.chinook_olap.DIM_Track` AS
  SELECT 
    t.TrackId AS dim_track_id, 
    t.Name AS track_name,
    al.Title AS album_name, 
    ar.name AS artist_name,
    t.composer AS composer,
    g.name AS genre,
    m.name AS mediatype
  FROM `final-dwh.chinook_stagging.track` t
  JOIN `final-dwh.chinook_stagging.album` al ON t.AlbumId = al.AlbumId
  JOIN `final-dwh.chinook_stagging.artist` ar ON al.ArtistId = ar.ArtistId
  JOIN `final-dwh.chinook_stagging.mediaType` m ON t.MediaTypeId = m.MediaTypeId
  JOIN `final-dwh.chinook_stagging.genre` g ON t.GenreId = g.GenreId;
