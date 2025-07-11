DROP TABLE IF EXISTS employee CASCADE;

-- Create the employee table
CREATE TABLE employee (
    -- Primary key: Employee ID
    id INTEGER PRIMARY KEY,

    -- Country code (2-character ISO code)
    country VARCHAR(2) NOT NULL,

    -- Employee personal information
    first_name VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL,
    gender CHAR(1) NOT NULL CHECK (gender IN ('M', 'F', 'O')),
    age INTEGER NOT NULL,

    -- Contact information
    email VARCHAR(255) NOT NULL,

    -- Audit timestamps
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    -- Employee status
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive'))
);

CREATE OR REPLACE FUNCTION update_employee_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at on row updates
CREATE TRIGGER trigger_employee_updated_at
    BEFORE UPDATE ON employee
    FOR EACH ROW
    EXECUTE FUNCTION update_employee_updated_at();
