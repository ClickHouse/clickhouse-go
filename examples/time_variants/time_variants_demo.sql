-- Drop table if exists
DROP TABLE IF EXISTS time_variants_demo;

-- Create table with all Time and Time64 variants
CREATE TABLE time_variants_demo (
    id UInt32,
    time_seconds Time,                    -- Time without precision (seconds)
    time64_3 Time64(3),                  -- Time64 with 3 decimal places (milliseconds)
    time64_6 Time64(6),                  -- Time64 with 6 decimal places (microseconds)
    time64_9 Time64(9),                  -- Time64 with 9 decimal places (nanoseconds)
    description String
) ENGINE = Memory;

-- Insert sample data
INSERT INTO time_variants_demo VALUES
(1, '12:30:45', '12:30:45.123', '12:30:45.123456', '12:30:45.123456789', 'Morning time'),
(2, '23:59:59', '23:59:59.999', '23:59:59.999999', '23:59:59.999999999', 'End of day'),
(3, '00:00:00', '00:00:00.000', '00:00:00.000000', '00:00:00.000000000', 'Start of day'),
(4, '15:30:20', '15:30:20.500', '15:30:20.500000', '15:30:20.500000000', 'Afternoon time'),
(5, '08:15:30', '08:15:30.750', '08:15:30.750000', '08:15:30.750000000', 'Morning time with precision');

-- Verify the data
SELECT 
    id,
    time_seconds,
    time64_3,
    time64_6,
    time64_9,
    description
FROM time_variants_demo
ORDER BY id; 