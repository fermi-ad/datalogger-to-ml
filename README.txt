#Use these commands inside stats: 1) sort tottime 2) stats 20

(base) ad130774-mlt:datalogger-to-ml snag$ python -m pstats dpmData.profile
Welcome to the profile statistics browser.
dpmData.profile% sort tottime
dpmData.profile% stats 20
Tue Jun 23 10:12:31 2020    dpmData.profile

         17180935 function calls (17105828 primitive calls) in 52.271 seconds

   Ordered by: internal time
   List reduced from 4952 to 20 due to restriction <20>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
     2660   14.215    0.005   14.215    0.005 {method '_g_flush' of 'tables.hdf5extension.Leaf' objects}
     2670    6.175    0.002    6.175    0.002 {tables.indexesextension.keysort}
  1338042    2.879    0.000    2.879    0.000 dpm_protocol.py:1034(consumeRawInt)
   101284    2.767    0.000    2.767    0.000 {method '_g_get_objinfo' of 'tables.hdf5extension.Group' objects}
     2670    2.497    0.001    2.563    0.001 {method '_fill_col' of 'tables.tableextension.Row' objects}
     7980    2.056    0.000    2.056    0.000 {method '_g_write_slice' of 'tables.hdf5extension.Array' objects}
    29290    1.937    0.000    2.283    0.000 {method '_g_setattr' of 'tables.hdf5extension.AttributeSet' objects}
     2670    1.635    0.001    1.848    0.001 /Users/snag/opt/anaconda3/lib/python3.7/site-packages/tables/index.py:636(final_idx32)
  1295420    1.100    0.000    1.341    0.000 dpm_protocol.py:1077(unmarshal_double)
  1303405    0.634    0.000    3.458    0.000 dpm_protocol.py:1070(unmarshal_int64)
     5322    0.465    0.000    5.245    0.001 dpm_protocol.py:1092(<listcomp>)
    40276    0.464    0.000    0.464    0.000 {method 'reduce' of 'numpy.ufunc' objects}
1132039/1126508    0.434    0.000    3.929    0.000 {built-in method builtins.getattr}
     5341    0.378    0.000    0.378    0.000 {pandas._libs.lib.maybe_convert_objects}
  1206555    0.340    0.000    0.539    0.000 {built-in method builtins.isinstance}
        3    0.339    0.113    0.339    0.113 {method '_read_records' of 'tables.tableextension.Table' objects}
95927/95912    0.276    0.000    3.888    0.000 /Users/snag/opt/anaconda3/lib/python3.7/site-packages/tables/group.py:697(_f_get_child)
     2740    0.250    0.000    0.250    0.000 {method 'acquire' of '_thread.lock' objects}
   130844    0.245    0.000    0.518    0.000 /Users/snag/opt/anaconda3/lib/python3.7/site-packages/tables/file.py:383(register_node)
  1295540    0.241    0.000    0.241    0.000 {built-in method _struct.unpack}
