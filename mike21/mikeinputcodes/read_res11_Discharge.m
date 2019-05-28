clc
clear all

NET.addAssembly('DHI.Generic.MikeZero.DFS');
import DHI.Generic.MikeZero.DFS.*;

infile = 'M11_Forcast.res11';

if (~exist(infile,'file'))
  [filename,filepath] = uigetfile('*.res11','Select the .res11');
  infile = [filepath,filename];
end
%MMD
% Data to extract, quantity, branch name and chainages
canals = {'KELANI', 'ST. SEBASTIAN NORTH CANAL', 'WELLAWATTA CNL', 'DEMATAGODA ELA', 'DEHIWARA CNL', 'KOTTE ELA SOUTH', 'PARLIAMENT LAKE 2', 'KELANI', 'MADIWALA EAST 3', 'KELANI', 'SALALIHINI MAWATHA', 'KELANI', 'KITTAMPAWWA', 'KOLONNAWA ELA', 'HEEN ELA', 'TORRINGTON CNL', 'PARLIAMENT BRIDGE 3', 'KOTTE ELA NORTH', 'KIRILLAPONE2', 'DEHIWARA CNL', 'MAHAWATTA ELA', 'HEEN ELA', 'KIRILLAPONE3', 'DEHIWARA CNL', 'MUTWALTUNNEL', 'WELLAWATTA CNL', 'PARLIAMENT LAKE 1', 'KOLONNAWA ELA', 'SERPENTINE', 'ST. SEBASTIAN SOUTH CANAL', 'ST. SEBASTIAN NORTH CANAL', 'MADIWALA EAST 2', 'MADIWALA EAST 3', 'KOLONNAWA ELA', 'KITTAMPAWWA', 'MALPURA TANK CANAL', 'SALALIHINI MAWATHA', 'SALALIHINI MAWATHA', 'KITTAMPAWWA', 'KITTAMPAWWA', 'SALALIHINI MAWATHA', 'D-E-F', 'KITTAMPAWWA', 'KITTAMPAWWA', 'PARLIAMENT_UPPER_REACH', 'PARLIAMENT LAKE', 'PARLIAMENT LAKE 2', 'SEA_PLANE_RUNWAY'};
chainages = [4224,1433.65,1123.38,2839,3314.66,292.388,983.45,15063.2,7386.45,10823.2,1924.43,5677.39,8955.8,3312.76,1769.58,986.468,75,124.955,706.938,3329.72,45.6883,1922.6,1600.77,241.455,758.074,27.7875,1135.17,416.36,1384.86,1702.89,903.95,770.5,3265.7,1285.69,7906.94,20,1424.12,2167.39,5626.55,194.549,370,3861.32,2379.2,4113.82,650,50,2880,81];
for i = 1:length(canals)
    extractPoints{i}.branchName = string(canals(i));
    extractPoints{i}.quantity = 'Discharge';
    extractPoints{i}.chainages = chainages(i);
end

[values, outInfos, times] = res11read(infile, extractPoints);

values = round(values,4);
timen = cellstr(datestr(times,'yyyy-mm-dd HH:MM:SS'));

TS = timeseries(values,timen);
%TS.TimeInfo.Units = 'days';
plot(TS)
%legend(outInfos{1}.quantity,outInfos{2}.quantity);

%%%%%%
%{
%Export data to a excel spreadsheet
outfilexl = 'resmike11_Discharge.xlsx';
col_headers = {'Time Stamp','Nagalagam Street River','Nagalagam Street','Wellawatta Canal-St Peters College','Dematagoda Canal-Orugodawatta','Dehiwala','Parliament Lake Bridge-Kotte Canal','Parliament Lake-Out','Ambatale River','Ambatale Outfall','Salalihini-River','Salalihini-Canal','Kittampahuwa River','Kalupalama','Yakbedda','Heen Ela','Torrinton','Parliament Lake','Kotte North Canal','OUSL-Nawala Kirulapana Canal','Dehiwala Canal','Near SLLRDC','Kirimandala Mw','OUSL-Narahenpita Rd','Swarna Rd-Wellawatta','Mutwal Outfall','Thummodara','JanakalaKendraya','Kotiyagoda','LesliRanagala Mw','Babapulle','Ingurukade Jn','Amaragoda','Malabe','Madinnagoda','Kittampahuwa','Weliwala Pond','Old Awissawella Rd','Kelani Mulla Outfall','Wellampitiya','Talatel Culvert','Wennawatta','Vivekarama Mw','Koratuwa Rd','Harwad Band','Kibulawala 1','Parlimant Lake Side','Polduwa-Parlimant Rd','Aggona'};
xlswrite(outfilexl, col_headers, 1,'A1');
xlswrite(outfilexl, timen, 1,'A2');
xlswrite(outfilexl, values, 1,'B2');
%}
valuescell = num2cell(values);

%Export data to a csv
outfilecsv = 'resmike11_Discharge.csv';
col_headers = {'Time Stamp','Nagalagam Street River','Nagalagam Street','Wellawatta Canal-St Peters College','Dematagoda Canal-Orugodawatta','Dehiwala','Parliament Lake Bridge-Kotte Canal','Parliament Lake-Out','Ambatale River','Ambatale Outfall','Salalihini-River','Salalihini-Canal','Kittampahuwa River','Kalupalama','Yakbedda','Heen Ela','Torrinton','Parliament Lake','Kotte North Canal','OUSL-Nawala Kirulapana Canal','Dehiwala Canal','Near SLLRDC','Kirimandala Mw','OUSL-Narahenpita Rd','Swarna Rd-Wellawatta','Mutwal Outfall','Thummodara','JanakalaKendraya','Kotiyagoda','LesliRanagala Mw','Babapulle','Ingurukade Jn','Amaragoda','Malabe','Madinnagoda','Kittampahuwa','Weliwala Pond','Old Awissawella Rd','Kelani Mulla Outfall','Wellampitiya','Talatel Culvert','Wennawatta','Vivekarama Mw','Koratuwa Rd','Harwad Band','Kibulawala 1','Parlimant Lake Side','Polduwa-Parlimant Rd','Aggona'};
matwrite = cell(length(timen)+1,length(col_headers));
matwrite(1,:) = col_headers;
matwrite(2:end,1) = timen;
matwrite(2:end,2:end) = valuescell;
tablewrite = cell2table(matwrite);
writetable(tablewrite,outfilecsv,'WriteVariableNames',0,'WriteRowNames',0)
fclose('all');

disp('done')
%MMD