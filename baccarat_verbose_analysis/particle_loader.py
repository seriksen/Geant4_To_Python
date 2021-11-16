import numpy as np
import tqdm
import multiprocessing as mp
import glob
import functools

def get_particle_dictionaries(npy_path, n_cores=1, particle='neutron', merge_multiples=True, add_filename=True, max_n_files=None):
    
    npy_files = glob.glob(npy_path + '/*')
    print("Base path {0}".format(npy_path))
    print("Found {0} npy files".format(len(npy_files)))
    
    if max_n_files is not None:
        if len(npy_files) > max_n_files:
            npy_files = npy_files[0:max_n_files]
    print("Processing {0} npy files".format(len(npy_files)))
    with mp.Pool(n_cores) as pool:
        result = list(tqdm.tqdm(pool.imap(functools.partial(extract_particle_from_dict, particle=particle,
                                                            merge_multiples=merge_multiples,
                                                            add_filename=add_filename),
                                                            npy_files)))
    print("Processing complete")
    print("N. {0} extracted: {1}".format(particle, len(result)))
    return result   
          

def extract_particle_from_dict(npy_file, particle='neutron', merge_multiples=True, add_filename=True):
    """
    This function returns all information about a given particle type for each event.
    For the GdLS study it is only useful for neutrons as it will only return one per event.
    However it has been written in a generalised way so that it can be used with other simulations in the future.

    If :code:`merge_multiples == True`, then the particle will be merged into a single dictionary. This was put in for
    neutrons, where the number of steps often goes above 50 and so is split into two separate entries. For other
    particles this option should probably not be used (unless they are primary particles - ie what started the
    simulation).

    :param npy_file: path to NPY file.
    :type npy_file: str
    :param particle: Particle to extract data of. eg 'neutron'
    :type particle: str
    :param merge_multiples: If True, will return a list[dict]. If False, will return a list[list[dict]]
    :type merge_multiples: bool
    :param add_filename: If True, add key entry pointing to the filename
    :type add_filename: bool
    :return: dictionaries of the particles
    :rtype: dict or list[dict]
    """
    particle_dictionary_list = []

    # Loop over each NPY file
    event_list = np.load(npy_file, allow_pickle=True)
    this_event_dictionaries = []
    for particle_dictionary in event_list:
        if particle_dictionary['particle'] == particle:
            this_event_dictionaries.append(particle_dictionary)
    del event_list
    # Now check to see the number of neutron entries
    if merge_multiples:
        if len(this_event_dictionaries) == 1:
            if add_filename:
                this_event_dictionaries[0]['filename'] = npy_file
            # return a dict
            particle_dictionary_list = this_event_dictionaries[0]
        else:
            # Merge dictionaries together
            merged_dictionary = {}
            for key in this_event_dictionaries[0].keys():
                key_data = []
                for particle_dictionary in this_event_dictionaries:
                    data = particle_dictionary[key]
                    key_data.append(data)
                if type(key_data[0]) == list:
                    key_data = [entry for sub_list in key_data for entry in sub_list]
                elif key == 'particle':
                    key_data = key_data[0]
                else:
                    key_data = key_data
                merged_dictionary[key] = key_data
            if add_filename:
                merged_dictionary['filename'] = npy_file
            # return a dict
            particle_dictionary_list = merged_dictionary
    else:
        if add_filename:
            for i in range(len(this_event_dictionaries)):
                this_event_dictionaries[i]['filename'] = npy_file
        # return a list of dict
        particle_dictionary_list = this_event_dictionaries

    return particle_dictionary_list