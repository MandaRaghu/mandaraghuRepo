-- Equipment Details By Supervisor, By Most Recently Completed Week Ending Date, DB Server: DMZ-SQL2016CL (10.1.2.13), Database: Timesheets
--																							MTA-SQLCL     (10.50.10.165), Database: Fleetwave
SELECT		DISTINCT
			TSH.WeekEndingDate, 
			EMP.AREA_SUPERVISOR_NUMBER, 
			EMP.AREA_SUPERVISOR_NAME, 
			TSE.EquipmentNumber,
			UL1.CODE_FW Unit_Class,
			UL1.DESCRIPTION_FW Class,
			VH.VEHICLE_TYPE_FW Unit_Type,
			VT.DESCRIPTION_FW Description,

			VH.Longitude_FW Longitude,
			VH.Latitude_FW Latitude,
			Case When VH.Third_eye_Installed_FW=1 OR VH.ZONAR_INSTALLED_FW=1 OR VH.SAMSARA_INSTALLED_FW=1 THEN 'Yes' Else 'No' END GPS_Installed,
			Case When VH.Third_eye_Installed_FW=1 THEN 'Third Eye ' ELSE '' END +
			Case When VH.ZONAR_INSTALLED_FW=1 THEN 'Zonar ' ELSE '' END +
			Case When VH.SAMSARA_INSTALLED_FW=1 Then 'Samsara ' ELSE '' END GPS_Company,
			VH.DRIVER_NAME_FW Fleetwave_AssignedTo,
			VH.PROJECT_ID_FW Fleetwave_AssignedJob
FROM		[DMZ-SQL2016CL].[Timesheets].dbo.TBL_T_TSHeader TSH
INNER JOIN	[DMZ-SQL2016CL].[Timesheets].dbo.TBL_T_TSCrew TSC
ON			TSH.HdrID = TSC.HdrID
INNER JOIN	[DMZ-SQL2016CL].[Timesheets].[dbo].TBL_M_EMPLOYEE_HIERARCHY EMP
ON			TSC.EmployeeNumber = Emp.PERSON_NUMBER
INNER JOIN	[DMZ-SQL2016CL].[Timesheets].[dbo].[TBL_T_TSEquipmentDetail] TSE
ON			TSC.HdrID = TSE.HdrId
INNER JOIN	[MTA-SQLCL].Fleetwave.dbo.VEHICLES_FW VH
ON			TSE.EquipmentNumber Collate Latin1_General_CI_AS= VH.VEHICLE_ID2_FW Collate Latin1_General_CI_AS
LEFT JOIN   [MTA-SQLCL].Fleetwave.dbo.VEHICLE_TYPES_FW VT
ON			VH.VEHICLE_TYPE_FW = VT.CODE_FW
LEFT JOIN   [MTA-SQLCL].Fleetwave.dbo.USER_LISTS1_FW UL1
ON			left(VH.VEHICLE_ID2_FW,2) = UL1.CODE_FW 
WHERE		TSH.WeekEndingDate = convert(date,GETDATE() -  (DATEPART(dw,GetDate()) - 1))
AND			TSC.CrewLeader = 1 AND rtrim(ltrim(COALESCE(EMP.AREA_SUPERVISOR_NUMBER, ''))) <> ''
--GROUP BY	TSH.WeekEndingDate, EMP.AREA_SUPERVISOR_NUMBER, EMP.AREA_SUPERVISOR_NAME
ORDER BY	TSH.WeekEndingDate, EMP.AREA_SUPERVISOR_NAME
