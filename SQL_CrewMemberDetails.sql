-- Details of Crew Members By Supervisor, By Most Recently Completed Week Ending Date, DB Server: DMZ-SQL2016CL (10.1.2.13), Database: Timesheets

SELECT		TSH.WeekEndingDate, 
			EMP.AREA_SUPERVISOR_NUMBER, 
			EMP.AREA_SUPERVISOR_NAME, 
			TSC.EmployeeNumber, 
			TSC.EmployeeName,
			TSH.JobNumber, 
			sum(coalesce(TSD.TotalHours,0)) Regular_Hours,
			sum(coalesce(TSD.TotalOvertimeHours,0)) Overtime_Hours
FROM		[DMZ-SQL2016CL].[Timesheets].dbo.TBL_T_TSHeader TSH
INNER JOIN	[DMZ-SQL2016CL].[Timesheets].dbo.TBL_T_TSCrew TSC
ON			TSH.HdrID = TSC.HdrID
INNER JOIN	[DMZ-SQL2016CL].[Timesheets].dbo.TBL_T_TSDetails TSD
ON			TSC.EmployeeNumber = TSD.EmployeeNumber and TSC.HdrID = TSD.HdrID
INNER JOIN	[DMZ-SQL2016CL].[Timesheets].[dbo].TBL_M_EMPLOYEE_HIERARCHY EMP
ON			TSC.EmployeeNumber = Emp.PERSON_NUMBER
WHERE		TSH.WeekEndingDate = convert(date,GETDATE() -  (DATEPART(dw,GetDate()) - 1))
AND			TSC.CrewLeader <> 1 AND rtrim(ltrim(COALESCE(EMP.AREA_SUPERVISOR_NUMBER, ''))) <> ''
AND			TSH.SendFusion_Flag = 1
GROUP BY	TSH.WeekEndingDate, 
			EMP.AREA_SUPERVISOR_NUMBER, 
			EMP.AREA_SUPERVISOR_NAME, 
			TSC.EmployeeNumber, 
			TSC.EmployeeName,
			TSH.JobNumber
ORDER BY	TSH.WeekEndingDate, EMP.AREA_SUPERVISOR_NAME, TSC.EmployeeName,TSH.JobNumber

