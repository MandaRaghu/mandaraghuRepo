-- Count of Crew Leaders By Supervisor, By Most Recently Completed Week Ending Date, DB Server: DMZ-SQL2016CL (10.1.2.13), Database: Timesheets

SELECT		TSH.WeekEndingDate, EMP.AREA_SUPERVISOR_NUMBER, EMP.AREA_SUPERVISOR_NAME, COUNT(DISTINCT PERSON_NUMBER) Crew_Leaders
FROM		[Timesheets].dbo.TBL_T_TSHeader TSH
INNER JOIN	[Timesheets].dbo.TBL_T_TSCrew TSC
ON			TSH.HdrID = TSC.HdrID
INNER JOIN	[Timesheets].[dbo].TBL_M_EMPLOYEE_HIERARCHY EMP
ON			TSC.EmployeeNumber = Emp.PERSON_NUMBER
WHERE		TSH.WeekEndingDate = convert(date,GETDATE() -  (DATEPART(dw,GetDate()) - 1))
AND			TSC.CrewLeader = 1 AND rtrim(ltrim(COALESCE(EMP.AREA_SUPERVISOR_NUMBER, ''))) <> ''
GROUP BY	TSH.WeekEndingDate, EMP.AREA_SUPERVISOR_NUMBER, EMP.AREA_SUPERVISOR_NAME
ORDER BY	TSH.WeekEndingDate