set schema 'map_$$operatore$$';

DROP TABLE IF EXISTS tbl_$$regione$$_contributi;


CREATE TABLE tbl_$$regione$$_contributi
(
    id_pixel integer,
    sito character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    azimut integer,
    frequenza integer,
    tecnologia character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    p_cover_dbm numeric(8,3) DEFAULT NULL::numeric
) PARTITION BY RANGE(frequenza, id_pixel);


CREATE TABLE tbl_$$regione$$_contributi_0700_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (700, 0) TO (700, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_0700_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (700, 8000000) TO (700, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_0700_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (700, 16000000) TO (700, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_0700_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (700, 24000000) TO (700, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_0800_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (800, 0) TO (800, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_0800_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (800, 8000000) TO (800, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_0800_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (800, 16000000) TO (800, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_0800_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (800, 24000000) TO (800, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_0900_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (900, 0) TO (900, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_0900_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (900, 8000000) TO (900, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_0900_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (900, 16000000) TO (900, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_0900_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (900, 24000000) TO (900, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_1400_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1400, 0) TO (1400, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_1400_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1400, 8000000) TO (1400, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_1400_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1400, 16000000) TO (1400, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_1400_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1400, 24000000) TO (1400, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_1500_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1500, 0) TO (1500, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_1500_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1500, 8000000) TO (1500, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_1500_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1500, 16000000) TO (1500, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_1500_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1500, 24000000) TO (1500, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_1800_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1800, 0) TO (1800, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_1800_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1800, 8000000) TO (1800, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_1800_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1800, 16000000) TO (1800, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_1800_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (1800, 24000000) TO (1800, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_2000_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2000, 0) TO (2000, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_2000_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2000, 8000000) TO (2000, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_2000_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2000, 16000000) TO (2000, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_2000_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2000, 24000000) TO (2000, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_2100_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2100, 0) TO (2100, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_2100_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2100, 8000000) TO (2100, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_2100_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2100, 16000000) TO (2100, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_2100_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2100, 24000000) TO (2100, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_2600_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2600, 0) TO (2600, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_2600_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2600, 8000000) TO (2600, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_2600_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2600, 16000000) TO (2600, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_2600_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (2600, 24000000) TO (2600, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_3600_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3600, 0) TO (3600, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_3600_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3600, 8000000) TO (3600, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_3600_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3600, 16000000) TO (3600, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_3600_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3600, 24000000) TO (3600, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_3700_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3700, 0) TO (3700, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_3700_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3700, 8000000) TO (3700, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_3700_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3700, 16000000) TO (3700, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_3700_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (3700, 24000000) TO (3700, 32000000);

CREATE TABLE tbl_$$regione$$_contributi_26000_08m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (26000, 0) TO (26000, 8000000);
CREATE TABLE tbl_$$regione$$_contributi_26000_16m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (26000, 8000000) TO (26000, 16000000);
CREATE TABLE tbl_$$regione$$_contributi_26000_24m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (26000, 16000000) TO (26000, 24000000);
CREATE TABLE tbl_$$regione$$_contributi_26000_32m PARTITION OF tbl_$$regione$$_contributi FOR VALUES FROM (26000, 24000000) TO (26000, 32000000);


CREATE INDEX idx_tbl_$$regione$$_contributi
    ON tbl_$$regione$$_contributi USING btree (frequenza ASC NULLS LAST, tecnologia ASC NULLS LAST, id_pixel ASC NULLS LAST) 
    INCLUDE (sito, azimut, p_cover_dbm) ;
