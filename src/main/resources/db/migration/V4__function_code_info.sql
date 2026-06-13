INSERT INTO function_code (code_id, func_code, func_name, memory_area, operation_type) VALUES
   (1, 1, 'Read Coils', 'COIL', 'READ'),
   (2, 2, 'Read Discrete Inputs', 'DISCRETE_INPUT', 'READ'),
   (3, 3, 'Read Holding Registers', 'HOLDING_REGISTER', 'READ'),
   (4, 4, 'Read Input Registers', 'INPUT_REGISTER', 'READ'),
   (5, 5, 'Write Single Coil', 'COIL', 'WRITE'),
   (6, 6, 'Write Single Register', 'HOLDING_REGISTER', 'WRITE'),
   (7, 15, 'Write Multiple Coils', 'COIL', 'WRITE'),
   (8, 16, 'Write Multiple Registers', 'HOLDING_REGISTER', 'WRITE');