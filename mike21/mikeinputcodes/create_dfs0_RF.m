clc
clear all
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
addpath('C:\Program Files\MATLAB\R2017a\mbin');
%dfsManager()
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%MMD
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%Read data file
infile = 'RF.txt';
D = readtable(infile, 'Delimiter', ',');
S = D{:,1};
R = [D{:,2:end}; zeros(1,width(D)-1)];
Rhedings = D.Properties.VariableNames(1,2:end);
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
fdt = S(1);
[fy,fM,fd] = ymd(fdt);
[fh,fm,fs] = hms(fdt);
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

NET.addAssembly('DHI.Generic.MikeZero.EUM');
NET.addAssembly('DHI.Generic.MikeZero.DFS');
H = NETaddDfsUtil();
import DHI.Generic.MikeZero.DFS.*;
import DHI.Generic.MikeZero.DFS.dfs123.*;
import DHI.Generic.MikeZero.*

% Flag specifying whether dfs0 file stores floats or doubles.
% MIKE Zero assumes floats, MIKE URBAN handles both.
useDouble = false;
%MMD
% Flag specifying wether to use the MatlabDfsUtil for writing, or whehter
% to use the raw DFS API routines. The latter is VERY slow, but required in
% case the MatlabDfsUtil.XXXX.dll is not available.
useUtil = ~isempty(H);

if (useDouble)
  dfsdataType = DfsSimpleType.Double;
else
  dfsdataType = DfsSimpleType.Float;
end

% Create a new dfs1 file
filename = 'KLB_Forcast.dfs0';

% Create an empty dfs1 file object
factory = DfsFactory();
builder = DfsBuilder.Create('KLB Rainfall_Forecast','Matlab DFS',0);

builder.SetDataType(0);
builder.SetGeographicalProjection(factory.CreateProjectionGeoOrigin('UTM-33',12,54,2.6));
builder.SetTemporalAxis(factory.CreateTemporalNonEqCalendarAxis(eumUnit.eumUsec,System.DateTime(fy,fM,fd,fh,fm,fs)));

% Write 120 time steps to the file, preallocate vector
% for time and a matrix for data for each item.
numTimes = length(S)+1;
times = zeros(numTimes,1);
data  = zeros(numTimes,1);

% Add items
items = cell(width(D)-1,1);
for n = 1:width(D)-1
    ID = char(Rhedings(n));
    items{n} = builder.CreateDynamicItemBuilder();
    items{n}.Set(ID, DHI.Generic.MikeZero.eumQuantity(eumItem.eumIRainfall,eumUnit.eumUmillimeter), dfsdataType);
    items{n}.SetValueType(DataValueType.StepAccumulated);
    items{n}.SetAxis(factory.CreateAxisEqD0());
    builder.AddDynamicItem(items{n}.GetDynamicItemInfo());
    
    % Create data vector, for each item
    data(:,n) = R(:,n);
    % Remove negative values from rain item
    data(data(:,n) < 0,1) = 0;
end

% Create the file - make it ready for data
builder.CreateFile(filename);
dfs = builder.GetFile();


% Create time vector - constant time step of 1 hr here
times(:) = 3600*(0:numTimes-1)';

% Put some date in the file
tic
if useUtil
  % Write to file using the MatlabDfsUtil
  MatlabDfsUtil.DfsUtil.WriteDfs0DataDouble(dfs, NET.convertArray(times), NET.convertArray(data, 'System.Double', size(data,1), size(data,2)))
else
  % Write to file using the raw .NET API (very slow)
  for i=1:numTimes
    if (useDouble)
        for j = 1:size(data,2)
            dfs.WriteItemTimeStepNext(times(i), NET.convertArray(data(i,j)));
        end
    else
        for j = 1:size(data,2)
            dfs.WriteItemTimeStepNext(times(i), NET.convertArray(single(data(i,j))));
        end
    end
    if (mod(i,100) == 0)
      fprintf('i = %i of %i\n',i,numTimes);
    end
  end
end
toc

dfs.Close();
fclose('all');
fprintf('\nFile created: ''%s''\n',filename);
%MMD