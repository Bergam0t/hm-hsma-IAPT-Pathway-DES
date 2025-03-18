import numpy as np

# function to vary the number of sessions
def vary_number_sessions(lower, upper, lambda_val=0.1):

        while True:
            # Generate a random number from the exponential distribution
            random_value = np.random.exponential(1 / lambda_val)

            # Shift the distribution to start at 'lower'
            sessions = random_value + lower #added lower to the random value.

            # Check if the generated value is within the desired range
            if lower <= sessions <= upper:
                # Convert to integer and return
                return int(sessions)
