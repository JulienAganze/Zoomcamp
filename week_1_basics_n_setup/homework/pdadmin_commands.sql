--Question 3. Count records
SELECT COUNT(1)
FROM green_taxi_trips
WHERE (CAST(lpep_dropoff_datetime AS DATE)='2019-01-15') and (CAST(lpep_pickup_datetime AS DATE)='2019-01-15');

--Question 4. Largest trip for each day
SELECT 
	CAST(lpep_pickup_datetime AS DATE),
	--CAST(lpep_pickup_datetime AS DATE) as "day",
	MAX(trip_distance)
FROM green_taxi_trips gt
GROUP BY 
	CAST(lpep_pickup_datetime AS DATE)
ORDER BY
	MAX(trip_distance) DESC;


--Question 5. The number of passengers
SELECT COUNT(1),
	passenger_count
	--CAST(lpep_pickup_datetime AS DATE),
	--CAST(lpep_pickup_datetime AS DATE) as "day",
	--MAX(trip_distance)
FROM green_taxi_trips gt
WHere
	--CAST(lpep_pickup_datetime AS DATE)='2019-01-01' AND passenger_count BETWEEN 2 and 3
	CAST(lpep_pickup_datetime AS DATE)='2019-01-01' AND (passenger_count=2 OR passenger_count=3)
GROUP BY 
	passenger_count


--Question 6. Largest tip
SELECT  gt."DOLocationID",
		--z."Zone",
		--z."LocationID",
		MAX(gt."tip_amount")
FROM green_taxi_trips gt
	JOIN zones z ON z."LocationID"=gt."PULocationID"
WHERE z."LocationID"=7
GROUP BY gt."DOLocationID"
ORDER BY MAX(gt."tip_amount") DESC;

