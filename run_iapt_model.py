from iapt_classes_trial import Trial

if __name__ == "__main__":
    my_trial = Trial()
    step2_results_df, step2_sessions_df, step3_results_df, step3_sessions_df, asst_weekly_dfs, step2_waiting_dfs, step3_waiting_dfs, staff_weekly_dfs, caseload_weekly_dfs  = my_trial.run_trial()

    # print(df_trial_results)
    # step2_waiting_dfs.to_csv("step2_waiters.csv", index=True)
    # step3_sessions_df.to_csv("step3_sessions.csv", index=True)
    # step2_results_df.to_csv("step2_results.csv", index=True)
    # caseload_weekly_dfs.to_csv("caseloads.csv", index=True)
    #step2_results_df, step3_results_df, df_trial_results, asst_weekly_dfs, step2_weekly_dfs, step3_weekly_dfs, staff_weekly_dfs, caseload_weekly_dfs  = my_trial.run_trial()

#df_trial_results, print(asst_weekly_dfs.to_string()), print(step2_weekly_dfs.to_string()), print(step3_weekly_dfs.to_string()), staff_weekly_dfs, print(caseload_weekly_dfs.to_string())

# asst_weekly_dfs.to_csv('S:\Departmental Shares\IM&T\Information\Business Intelligence\Heath McDonald\HSMA\Discrete Event Simulations\IAPT DES\asst_weekly_summary.csv')
# step2_weekly_dfs.to_csv('step2_weekly_summary.csv')
# step3_weekly_dfs.to_csv('step3_weekly_summary.csv')
# caseload_weekly_dfs.to_csv('caseload_weekly_summary.csv')

#S:\Departmental Shares\IM&T\Information\Business Intelligence\Heath McDonald\HSMA\Discrete Event Simulations\IAPT DES
