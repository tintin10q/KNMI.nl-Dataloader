import h5py

filename = "KMDS__OPER_P___10M_OBS_L2_202405161720.nc"

h5 = h5py.File(filename,'r')


# futures_data = h5['futures_data']  # VSTOXX futures data
# options_data = h5['options_data']  # VSTOXX call option data

for key in h5.keys():
    # print(key)
    # print(h5[key])
    # print(help(h5["overview"]))
    for name, value in h5["overview"].attrs.items():
        print(name,value)
    # require_group
    # visit!
    break
    print(name, help(value))
    for items in value.items():
        for item in items:
            ...
h5.close()


def hi():
    return "hi"