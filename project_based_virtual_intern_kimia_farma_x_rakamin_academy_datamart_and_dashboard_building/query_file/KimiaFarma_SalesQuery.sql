--------------------------------------
---------- TABLE DEFINITION ----------
--------------------------------------
-- To create the "penjualan" table
DROP TABLE IF EXISTS penjualan;

CREATE TABLE public.penjualan(
	id_distributor VARCHAR(3),
	id_cabang VARCHAR(5),
	id_invoice VARCHAR(6) PRIMARY KEY,
	tanggal DATE,
	id_customer VARCHAR(9),
	id_barang VARCHAR(7),
	jumlah_barang INT,
	unit VARCHAR(3),
	harga NUMERIC,
	mata_uang VARCHAR(3),
	brand_id VARCHAR(7),
	lini VARCHAR(8)
);

-- To create the "pelanggan" table
DROP TABLE IF EXISTS pelanggan;

CREATE TABLE public.pelanggan(
	id_customer VARCHAR(9) PRIMARY KEY,
	lvl VARCHAR(7),
	nama VARCHAR(17), 
	id_cabang_sales VARCHAR(5),
	cabang_sales VARCHAR(9),
	id_grp VARCHAR(3),
	grp VARCHAR(6)
);

-- To create the "barang" table
DROP TABLE IF EXISTS barang;

CREATE TABLE public.barang(
	kode_barang VARCHAR(7) PRIMARY KEY,
	sektor CHAR(1),
	nama_barang VARCHAR(41),
	tipe CHAR(4),
	nama_tipe CHAR(11),
	kode_lini CHAR(3),
	lini VARCHAR(8),
	kemasan VARCHAR(6)
);

--------------------------------------
---------- TABLE INSERTION -----------
--------------------------------------
-- To insert values into the "penjualan" table
COPY public.penjualan(
	id_distributor,
	id_cabang,
	id_invoice,
	tanggal,
	id_customer,
	id_barang,
	jumlah_barang,
	unit,
	harga,
	mata_uang,
	brand_id,
	lini
)
FROM 'D:\COURSES\Data Analytics\Rakamin\PBI\Kimia Farma Big Data Analytics\Week 4\Final Task\Dataset\Penjualan.csv'
DELIMITER ';'
CSV HEADER;

-- To insert values into the "pelanggan" table
COPY public.pelanggan(
	id_customer,
	lvl,
	nama, 
	id_cabang_sales,
	cabang_sales,
	id_grp,
	grp
)
FROM 'D:\COURSES\Data Analytics\Rakamin\PBI\Kimia Farma Big Data Analytics\Week 4\Final Task\Dataset\Pelanggan.csv'
DELIMITER ';'
CSV HEADER;

-- To insert values into the "barang" table
COPY public.barang(
	kode_barang,
	sektor,
	nama_barang,
	tipe,
	nama_tipe,
	kode_lini,
	lini,
	kemasan
)
FROM 'D:\COURSES\Data Analytics\Rakamin\PBI\Kimia Farma Big Data Analytics\Week 4\Final Task\Dataset\Barang.csv'
DELIMITER ';'
CSV HEADER;

--------------------------------------
-------- TABLE TRANSFORMATION --------
--------------------------------------
-- Adding the total_sales column in the penjualan table
ALTER TABLE penjualan
ADD COLUMN total_sales NUMERIC;

UPDATE penjualan
SET total_sales = jumlah_barang * harga;

----------------------------------------------------------
-------- DATAMART CREATION: SALES DATAMART (BASE) --------
----------------------------------------------------------
CREATE TABLE public.sales_base_table AS(
	SELECT pj.id_invoice,
		   pj.tanggal,
		   pl.id_customer,
		   pl.nama AS nama_customer,
		   pl.id_cabang_sales AS id_lokasi_cabang,
		   pl.cabang_sales AS lokasi_cabang,
		   pl.id_grp AS id_tipe_cabang,
		   pl.grp AS tipe_cabang,
		   b.kode_barang,
	       b.nama_barang,
		   b.kode_lini AS id_brand,
		   b.lini AS nama_brand,
		   b.kemasan,
		   pj.jumlah_barang,
	       pj.harga,
		   pj.total_sales AS total_harga
	FROM penjualan AS pj
	LEFT JOIN barang AS b
	ON pj.id_barang = b.kode_barang
	LEFT JOIN pelanggan AS pl
	USING (id_customer)
)

---------------------------------------------------------------------
-------- DATAMART CREATION: MONTHLY SALES DATA (AGGREGATED) ---------
---------------------------------------------------------------------
CREATE TABLE monthly_sales_data AS (
	SELECT EXTRACT(MONTH FROM tanggal)::INT AS bulan,
		   SUM(jumlah_barang) AS jumlah_terjual,
		   ROUND(AVG(harga), 2) AS average_harga_produk,
		   SUM(total_harga) AS total_sales
	FROM sales_base_table
	GROUP BY 1
	ORDER BY 1
);

---------------------------------------------------------------------
-------- DATAMART CREATION: MONTHLY TOP CUSTOMER (AGGREGATED) -------
---------------------------------------------------------------------
CREATE TABLE monthly_top_customer AS (
	SELECT bulan,
		   DENSE_RANK() OVER (PARTITION BY bulan ORDER BY total_sales DESC) AS cust_rank,
		   nama_customer,
		   lokasi_cabang,
		   tipe_cabang,
		   total_sales
	FROM(
		SELECT EXTRACT(MONTH FROM tanggal)::INT AS bulan,
			   nama_customer,
			   lokasi_cabang,
			   tipe_cabang,
			   ROUND(SUM(total_harga), 2) AS total_sales
		FROM sales_base_table
		GROUP BY 1, 2, 3, 4) AS tmp1
	ORDER BY 1, 2
);

---------------------------------------------------------------------
-------- DATAMART CREATION: MONTHLY TOP PRODUCTS (AGGREGATED) -------
---------------------------------------------------------------------
CREATE TABLE monthly_top_products AS (
	SELECT bulan,
		   DENSE_RANK() OVER (PARTITION BY bulan ORDER BY total_sales DESC) AS product_rank,
		   nama_barang, 
		   nama_brand,
		   harga_satuan,
		   total_terjual,
		   total_sales
	FROM (
		SELECT EXTRACT(MONTH FROM tanggal)::INT AS bulan,
			   nama_barang,
			   nama_brand,
			   harga AS harga_satuan,
			   SUM(jumlah_barang) AS total_terjual,
			   ROUND(SUM(total_harga), 2) AS total_sales
		FROM sales_base_table
		GROUP BY 1, 2, 3, 4
		) AS tmp1
	ORDER BY 1, 2
);