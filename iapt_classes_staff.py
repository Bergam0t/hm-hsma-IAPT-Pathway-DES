# Staff class to be run weekly by week runner to record staff non-clinical time
class Staff:
    def __init__(self, s_id):

        # Staff attributes
        self.id = s_id
        self.week_number = 0  # the week number the staff activity is being recorded for
        self.staff_type = []  # what type of staff i.e. CBT, PwP, Couns
        self.staff_hours_avail = 0  # how many hours p/w they work
        self.staff_band = 0  # what staff band

        self.staff_time_wellbg = 0  # staff time for wellbeing
        self.staff_time_superv = 0  # staff time for supervision
        self.staff_time_breaks = 0  # staff time for breaks
        self.staff_time_huddle = 0  # staff time for counsellor huddle
        self.staff_time_cpd = 0  # staff time for CPD
