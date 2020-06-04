Create PROCEDURE dbo.SP_TBL_Hist_Pjb_Briefing_Refresh
AS
BEGIN

    SET NOCOUNT ON

   insert into TBL_Hist_PJB_Briefing
	select * from vw_PJB_Briefing s
	where not exists
			(
				select 1 from TBL_Hist_PJB_Briefing t where 
					s.[WeekEndingDate]=t.[WeekEndingDate] and s.[Regional_VP_NAME]=t.[Regional_VP_NAME]
					and s.[Operations_VP_Emp_Name]=t.[Operations_VP_Emp_Name] and s.[Area_Supervisor_Name]=t.[Area_Supervisor_Name]
					and s.[Area_Supervisor_Number]=t.[Area_Supervisor_Number] and s.[EMAIL_ADDRESS]=t.[EMAIL_ADDRESS] 
					and s.[OVP_EMAIL_ADDRESS]=t.[OVP_EMAIL_ADDRESS] and s.[EmployeeName]=t.[EmployeeName] 
					and s.[EmployeeNumber]=t.[EmployeeNumber] and s.[Monday]=t.[Monday] 
					and s.[Tuesday]=t.[Tuesday] and s.[Wednesday]=t.[Wednesday] 
					and s.[Thursday]=t.[Thursday] and s.[Friday]=t.[Friday] 
					and s.[Saturday]=t.[Saturday] and s.[Sunday]=t.[Sunday] and s.[Status]=t.[Status] 
			)
END
GO