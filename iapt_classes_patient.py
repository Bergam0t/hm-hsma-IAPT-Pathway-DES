# Patient to capture flow of patient through pathway
class Patient:
    def __init__(self, p_id):
        # Patient
        self.id = p_id
        self.patient_at_risk = 0  # used to determine DNA/Canx policy to apply

        # Week they were added to the waiting list (for debugging purposes)
        self.week_added = None

        # Referral
        self.referral_rejected = 0  # were they rejected at referral
        self.referral_time_screen = 0  # time taken to screen referral
        self.referral_req_review = 0  # does the ref need to go to MDT review
        self.referral_review_rej = 0
        self.time_to_mdt = 0  # how long to MDT, max 2 weeks
        self.opted_in = 0  # did the patient opt in or not
        self.opt_in_wait = 0  # how much of 1 week opt-in window was used
        self.opt_in_qtime = 0  # how much of 4 week TA app window was used
        self.attended_ta = 0  # did the patient attend TA appointment

        self.initial_step = []  # string, whether they were step 2 or step 3

        # Step2
        # identifier for the staff member allocated to their treatment
        self.step2_resource_id = []
        self.step2_path_route = []  # string, which Step2 path they took
        self.step2_place_on_wl = 0  # position they are on Step2 waiting list
        self.step2_wait_week = 0  # week they started waiting to enter treatment
        self.step2_start_week = 0  # the week number they started treatment
        self.step2_session_count = 0  # counter for no. of sessions have had
        self.step2_drop_out = 0  # did they drop out during Step2
        self.step2_week_number = 0  # counter for which week number they are on
        self.step2_end_week = 0  # the week number when they completed treatment

        # Step3
        # identifier for the staff member allocated to their treatment
        self.step3_resource_id = []
        self.step3_path_route = []  # string, which Step2 path they took
        self.step3_place_on_wl = 0  # position they are on Step2 waiting list
        self.step2_wait_week = 0  # week they started waiting to enter treatment
        self.step3_start_week = 0
        self.step3_session_count = 0  # counter for no. of sessions have had
        self.step3_drop_out = 0  # did they drop out during Step2
        self.step3_week_number = 0  # counter for which week number they are on
        self.step3_end_week = 0
