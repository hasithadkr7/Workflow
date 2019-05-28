clc
clear all
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
addpath('C:\Program Files\MATLAB\R2017a\mbin');
%dfsManager()
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%MMD
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%Read data file
infile = fopen('Discharge.txt');
if infile>0
    D = textscan(infile, '%s %f', 'Delimiter',',');
    fclose(infile);
end
S = datetime(D{1}, 'InputFormat', 'yyyy-MM-dd HH:mm:ss');
R = [D{2}];
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
filename = 'Ambathale Discharge(Flo-2D 250m).dfs0';

% Create an empty dfs1 file object
factory = DfsFactory();
builder = DfsBuilder.Create('Ambathale Discharge(Flo-2D 250m)','Matlab DFS',0);

builder.SetDataType(0);
builder.SetGeographicalProjection(factory.CreateProjectionGeoOrigin('UTM-33',12,54,2.6));
builder.SetTemporalAxis(factory.CreateTemporalNonEqCalendarAxis(eumUnit.eumUsec,System.DateTime(fy,fM,fd,fh,fm,fs)));

% Add items
item1 = builder.CreateDynamicItemBuilder();
item1.Set('Discharge', DHI.Generic.MikeZero.eumQuantity(eumItem.eumIDischarge,eumUnit.eumUm3PerSec), dfsdataType);
item1.SetValueType(DataValueType.Instantaneous);
item1.SetAxis(factory.CreateAxisEqD0());
builder.AddDynamicItem(item1.GetDynamicItemInfo());

% Create the file - make it ready for data
builder.CreateFile(filename);
dfs = builder.GetFile();

% Write 481 time steps to the file, preallocate vector
% for time and a matrix for data for each item.
numTimes = 481;
times = zeros(numTimes,1);
data  = zeros(numTimes,1);

% Create time vector - constant time step of 15 min here
times(:) = 900*(0:numTimes-1)';

% Create data vector, for each item
data(:,1) = R;
% Remove negative values from rain item
data(data(:,1) < 0,1) = 0;

% Put some date in the file
tic
if useUtil
  % Write to file using the MatlabDfsUtil
  MatlabDfsUtil.DfsUtil.WriteDfs0DataDouble(dfs, NET.convertArray(times), NET.convertArray(data, 'System.Double', size(data,1), size(data,2)))
else
  % Write to file using the raw .NET API (very slow)
  for i=1:numTimes
    if (useDouble)
      dfs.WriteItemTimeStepNext(times(i), NET.convertArray(data(i,1))); 
    else
      dfs.WriteItemTimeStepNext(times(i), NET.convertArray(single(data(i,1)))); 
    end
    if (mod(i,100) == 0)
      fprintf('i = %i of %i\n',i,numTimes);
    end
  end
end
toc

dfs.Close();

fprintf('\nFile created: ''%s''\n',filename);
