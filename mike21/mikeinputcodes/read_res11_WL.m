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
chainages = [4294, 1422.52, 1129.93, 2849.06, 3309.66, 267.59, 1072.2, 15078, 7391.64, 10838, 1949.43, 5672.16, 8955, 3304.27, 1794.95, 995.62, 150, 109.01, 719.76, 3339.78, 49.99, 1921.64, 1600, 278.76, 750, 0, 1138.94, 413.23, 1336.71, 1707.89, 905.97, 770.28, 3280.09, 1299.71, 7908, 0, 1424, 2167, 5626, 193.148, 369.5, 3873.64, 2378, 4113, 600, 0, 2885, 0];
for i = 1:length(canals)
    extractPoints{i}.branchName = string(canals(i));
    extractPoints{i}.quantity = 'Water Level';
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
outfilexl = 'resmike11_WL.xlsx';
col_headers = {'Time Stamp','Nagalagam Street River','Nagalagam Street','Wellawatta Canal-St Peters College','Dematagoda Canal-Orugodawatta','Dehiwala','Parliament Lake Bridge-Kotte Canal','Parliament Lake-Out','Ambatale River','Ambatale Outfall','Salalihini-River','Salalihini-Canal','Kittampahuwa River','Kalupalama','Yakbedda','Heen Ela','Torrinton','Parliament Lake','Kotte North Canal','OUSL-Nawala Kirulapana Canal','Dehiwala Canal','Near SLLRDC','Kirimandala Mw','OUSL-Narahenpita Rd','Swarna Rd-Wellawatta','Mutwal Outfall','Thummodara','JanakalaKendraya','Kotiyagoda','LesliRanagala Mw','Babapulle','Ingurukade Jn','Amaragoda','Malabe','Madinnagoda','Kittampahuwa','Weliwala Pond','Old Awissawella Rd','Kelani Mulla Outfall','Wellampitiya','Talatel Culvert','Wennawatta','Vivekarama Mw','Koratuwa Rd','Harwad Band','Kibulawala 1','Parlimant Lake Side','Polduwa-Parlimant Rd','Aggona'};
xlswrite(outfilexl, col_headers, 1,'A1');
xlswrite(outfilexl, timen, 1,'A2');
xlswrite(outfilexl, values, 1,'B2');
%}
valuescell = num2cell(values);

%Export data to a csv
outfilecsv = 'resmike11_WL.csv';
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