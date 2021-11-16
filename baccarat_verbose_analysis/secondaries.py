import multiprocessing as mp
import functools
import tqdm
import numpy as np

def get_secondary(primary_dict, event_dict, primary_track_id=None, use_position=False):
    
    if primary_track_id is None:
        parent_id = primary_dict['track_id']
    else:
        parent_id = primary_track_id
    secondaries = []
    for particle_dict in event_dict:
        if particle_dict['parent_id'] == parent_id:
            if use_position:
                particle_xyz = [particle_dict['x_mm'][0], particle_dict['y_mm'][0], particle_dict['z_mm'][0]]
                primary_xyz = [primary_dict['x_mm'][-1], primary_dict['y_mm'][-1], primary_dict['z_mm'][-1]]
                if particle_xyz == primary_xyz:
                    secondaries.append(particle_dict)
            else:
                secondaries.append(particle_dict)
    if len(secondaries) > 0:
        new_secondaries = []
        for s in secondaries:
            a =  get_secondary(s, event_dict)
            new_secondaries.extend(a)
        secondaries.extend(new_secondaries)
    return secondaries


def get_neutron_secondaries(neutron_dict):
    
    secondary_dicts = []
    event_dict = np.load(neutron_dict['filename'], allow_pickle=True)
    if type(neutron_dict['track_id']) == list:
        parent_id = neutron_dict['track_id'][-1]
    else:
        parent_id = neutron_dict['track_id']
    
    secondary_tracks = []
    secondaries = []
    primary_xyz = [neutron_dict['x_mm'][-1], neutron_dict['y_mm'][-1], neutron_dict['z_mm'][-1]]
    n_particles = len(event_dict)
    for particle_dict in event_dict:
        if particle_dict['parent_id'] == parent_id:
            try:
                particle_xyz = [particle_dict['x_mm'][0], particle_dict['y_mm'][0], particle_dict['z_mm'][0]]
                if particle_xyz == primary_xyz:
                    secondaries.append(particle_dict)
                    secondary_tracks.append(particle_dict['track_id'])
            except:
                q = -1
    n_primary_secondaries = len(secondary_tracks)
    for particle_dict in event_dict:
        if particle_dict['parent_id'] in secondary_tracks:
            secondaries.append(particle_dict)
            secondary_tracks.append(particle_dict['track_id'])
    n_total_secondaries = len(secondary_tracks)
    
    
    track_ids = []
    parent_ids = []
    for e in secondaries:
        track_ids.append(e['track_id'])
        parent_ids.append(e['parent_id'])
    parent_ids = np.unique(parent_ids)
    final_secondaries = []
    for e in secondaries:
        if e['track_id'] not in parent_ids:
            final_secondaries.append(e)
    n_final_secondaries = len(final_secondaries)
    
    final_optical_photons = []
    n_od_pmt_end = 0
    for e in final_secondaries:
        if e['particle'] == 'opticalphoton':
            final_optical_photons.append(e)
            if type(e['NextVolume']) == list:
                final_pos = e['NextVolume'][-1]
            else:
                final_pos = e['NextVolume']
            if 'Water_PMT_' in final_pos:
                n_od_pmt_end += 1
                
    n_final_optical_photons = len(final_optical_photons)
        
    return [n_particles, n_primary_secondaries, n_total_secondaries, n_final_secondaries, n_final_optical_photons, n_od_pmt_end]




######################
## Neutron Captures ##
######################

def get_neutron_capture_particle(primary_particle_dictionary):
    
    capture_particle = None
    final_event = None
    final_volume = None
    #primary_particle_dictionary = all_neutron_dicts[0]
    # Can either be a list or a str depending on if merged or not
    primary_track_id = primary_particle_dictionary['track_id']
    # Get list[str] of secondaries
    if type(primary_track_id) == list:
        track_id = primary_track_id[-1]
    else:
        track_id = primary_track_id

    primary_xyz = [primary_particle_dictionary['x_mm'][-1], primary_particle_dictionary['y_mm'][-1], primary_particle_dictionary['z_mm'][-1]]

    # Load event dictionaries
    event_list = np.load(primary_particle_dictionary['filename'], allow_pickle=True)

    for particle_dictionary in event_list:
        # Get final 
        if particle_dictionary['track_id'] == track_id:
            try:
                final_event = particle_dictionary['ProcName'][-1]
                final_volume = particle_dictionary['NextVolume'][-1]
            except:
                return None, None, None
        if particle_dictionary['parent_id'] == track_id:
            # Check particle
            particle = particle_dictionary['particle']
            # Ignore gamma and electrons
            if particle != 'gamma' and particle != 'e-':# and particle != 'C13' and particle != 'C14':
                # check x-y-z
                try:
                    particle_xyz = [particle_dictionary['x_mm'][0], particle_dictionary['y_mm'][0], particle_dictionary['z_mm'][0]]
                except:
                    particle_xyz = [None, None, None]
                if particle_xyz == primary_xyz:
                    capture_particle = particle_dictionary['particle']
    
    return final_volume, final_event, capture_particle


def get_neutron_captures(primary_particle_dictionary, which_particles_to_keep):
    
    capture_particle = None
    final_event = None
    final_volume = None
    
    if type(which_particles_to_keep) == str:
        which_particles_to_keep = [which_particles_to_keep]
    
    #primary_particle_dictionary = all_neutron_dicts[0]
    # Can either be a list or a str depending on if merged or not
    primary_track_id = primary_particle_dictionary['track_id']
    # Get list[str] of secondaries
    if type(primary_track_id) == list:
        track_id = primary_track_id[-1]
    else:
        track_id = primary_track_id

    primary_xyz = [primary_particle_dictionary['x_mm'][-1], primary_particle_dictionary['y_mm'][-1], primary_particle_dictionary['z_mm'][-1]]

    # Load event dictionaries
    event_list = np.load(primary_particle_dictionary['filename'], allow_pickle=True)

    for particle_dictionary in event_list:
        # Get final 
        if particle_dictionary['track_id'] == track_id:
            try:
                final_event = particle_dictionary['ProcName'][-1]
                final_volume = particle_dictionary['NextVolume'][-1]
            except:
                return None
        if particle_dictionary['parent_id'] == track_id:
            # Check particle
            particle = particle_dictionary['particle']
            # Ignore gamma and electrons
            for p in which_particles_to_keep:
                if particle == p:
                    # check x-y-z
                    try:
                        particle_xyz = [particle_dictionary['x_mm'][0], particle_dictionary['y_mm'][0], particle_dictionary['z_mm'][0]]
                    except:
                        particle_xyz = [None, None, None]
                    if particle_xyz == primary_xyz:
                        capture_particle = particle_dictionary['particle']
    
    if capture_particle is not None:
        return primary_particle_dictionary
    else:
        return None