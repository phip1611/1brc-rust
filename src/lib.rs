//! Implements a highly optimized variant for the 1BRC.
//! Look at [`process_file_chunk`], which is the heart of the implementation.
//!
//! All convenience around it, such as allocating a few helpers, is negligible
//! from my testing.

mod aggregated_data;
mod chunk_iter;

use crate::chunk_iter::ChunkIter;
use crate::data_set_properties::{
    ALL_STATIONS, MIN_MEASUREMENT_LEN, MIN_STATION_LEN, STATIONS_IN_DATASET,
};
use aggregated_data::AggregatedData;
use fnv::FnvHashMap as HashMap;
use memmap::{Mmap, MmapOptions};
use ptr_hash::{PtrHash, PtrHashParams};
use std::fs::File;
use std::hint::black_box;
use std::io::Write;
use std::path::Path;
use std::str::from_utf8_unchecked;
use std::thread::available_parallelism;
use std::{slice, thread};

/// Some characteristics specifically to the [1BRC data set](https://github.com/gunnarmorling/1brc/blob/db064194be375edc02d6dbcd21268ad40f7e2869/src/main/java/dev/morling/onebrc/CreateMeasurements.java).
mod data_set_properties {
    /// The amount of distinct weather stations (cities).
    pub const STATIONS_IN_DATASET: usize = 413;
    /// The minimum station name length (for example: `Jos`).
    pub const MIN_STATION_LEN: usize = 3;
    /// The minimum measurement (str) len (for example: `6.6`).
    pub const MIN_MEASUREMENT_LEN: usize = 3;

    pub const ALL_STATIONS: [&'static str; STATIONS_IN_DATASET] = [
        "Abha",
        "Abidjan",
        "Abéché",
        "Accra",
        "Addis Ababa",
        "Adelaide",
        "Aden",
        "Ahvaz",
        "Albuquerque",
        "Alexandra",
        "Alexandria",
        "Algiers",
        "Alice Springs",
        "Almaty",
        "Amsterdam",
        "Anadyr",
        "Anchorage",
        "Andorra la Vella",
        "Ankara",
        "Antananarivo",
        "Antsiranana",
        "Arkhangelsk",
        "Ashgabat",
        "Asmara",
        "Assab",
        "Astana",
        "Athens",
        "Atlanta",
        "Auckland",
        "Austin",
        "Baghdad",
        "Baguio",
        "Baku",
        "Baltimore",
        "Bamako",
        "Bangkok",
        "Bangui",
        "Banjul",
        "Barcelona",
        "Bata",
        "Batumi",
        "Beijing",
        "Beirut",
        "Belgrade",
        "Belize City",
        "Benghazi",
        "Bergen",
        "Berlin",
        "Bilbao",
        "Birao",
        "Bishkek",
        "Bissau",
        "Blantyre",
        "Bloemfontein",
        "Boise",
        "Bordeaux",
        "Bosaso",
        "Boston",
        "Bouaké",
        "Bratislava",
        "Brazzaville",
        "Bridgetown",
        "Brisbane",
        "Brussels",
        "Bucharest",
        "Budapest",
        "Bujumbura",
        "Bulawayo",
        "Burnie",
        "Busan",
        "Cabo San Lucas",
        "Cairns",
        "Cairo",
        "Calgary",
        "Canberra",
        "Cape Town",
        "Changsha",
        "Charlotte",
        "Chiang Mai",
        "Chicago",
        "Chihuahua",
        "Chittagong",
        "Chișinău",
        "Chongqing",
        "Christchurch",
        "City of San Marino",
        "Colombo",
        "Columbus",
        "Conakry",
        "Copenhagen",
        "Cotonou",
        "Cracow",
        "Da Lat",
        "Da Nang",
        "Dakar",
        "Dallas",
        "Damascus",
        "Dampier",
        "Dar es Salaam",
        "Darwin",
        "Denpasar",
        "Denver",
        "Detroit",
        "Dhaka",
        "Dikson",
        "Dili",
        "Djibouti",
        "Dodoma",
        "Dolisie",
        "Douala",
        "Dubai",
        "Dublin",
        "Dunedin",
        "Durban",
        "Dushanbe",
        "Edinburgh",
        "Edmonton",
        "El Paso",
        "Entebbe",
        "Erbil",
        "Erzurum",
        "Fairbanks",
        "Fianarantsoa",
        "Flores,  Petén",
        "Frankfurt",
        "Fresno",
        "Fukuoka",
        "Gaborone",
        "Gabès",
        "Gagnoa",
        "Gangtok",
        "Garissa",
        "Garoua",
        "George Town",
        "Ghanzi",
        "Gjoa Haven",
        "Guadalajara",
        "Guangzhou",
        "Guatemala City",
        "Halifax",
        "Hamburg",
        "Hamilton",
        "Hanga Roa",
        "Hanoi",
        "Harare",
        "Harbin",
        "Hargeisa",
        "Hat Yai",
        "Havana",
        "Helsinki",
        "Heraklion",
        "Hiroshima",
        "Ho Chi Minh City",
        "Hobart",
        "Hong Kong",
        "Honiara",
        "Honolulu",
        "Houston",
        "Ifrane",
        "Indianapolis",
        "Iqaluit",
        "Irkutsk",
        "Istanbul",
        "Jacksonville",
        "Jakarta",
        "Jayapura",
        "Jerusalem",
        "Johannesburg",
        "Jos",
        "Juba",
        "Kabul",
        "Kampala",
        "Kandi",
        "Kankan",
        "Kano",
        "Kansas City",
        "Karachi",
        "Karonga",
        "Kathmandu",
        "Khartoum",
        "Kingston",
        "Kinshasa",
        "Kolkata",
        "Kuala Lumpur",
        "Kumasi",
        "Kunming",
        "Kuopio",
        "Kuwait City",
        "Kyiv",
        "Kyoto",
        "La Ceiba",
        "La Paz",
        "Lagos",
        "Lahore",
        "Lake Havasu City",
        "Lake Tekapo",
        "Las Palmas de Gran Canaria",
        "Las Vegas",
        "Launceston",
        "Lhasa",
        "Libreville",
        "Lisbon",
        "Livingstone",
        "Ljubljana",
        "Lodwar",
        "Lomé",
        "London",
        "Los Angeles",
        "Louisville",
        "Luanda",
        "Lubumbashi",
        "Lusaka",
        "Luxembourg City",
        "Lviv",
        "Lyon",
        "Madrid",
        "Mahajanga",
        "Makassar",
        "Makurdi",
        "Malabo",
        "Malé",
        "Managua",
        "Manama",
        "Mandalay",
        "Mango",
        "Manila",
        "Maputo",
        "Marrakesh",
        "Marseille",
        "Maun",
        "Medan",
        "Mek'ele",
        "Melbourne",
        "Memphis",
        "Mexicali",
        "Mexico City",
        "Miami",
        "Milan",
        "Milwaukee",
        "Minneapolis",
        "Minsk",
        "Mogadishu",
        "Mombasa",
        "Monaco",
        "Moncton",
        "Monterrey",
        "Montreal",
        "Moscow",
        "Mumbai",
        "Murmansk",
        "Muscat",
        "Mzuzu",
        "N'Djamena",
        "Naha",
        "Nairobi",
        "Nakhon Ratchasima",
        "Napier",
        "Napoli",
        "Nashville",
        "Nassau",
        "Ndola",
        "New Delhi",
        "New Orleans",
        "New York City",
        "Ngaoundéré",
        "Niamey",
        "Nicosia",
        "Niigata",
        "Nouadhibou",
        "Nouakchott",
        "Novosibirsk",
        "Nuuk",
        "Odesa",
        "Odienné",
        "Oklahoma City",
        "Omaha",
        "Oranjestad",
        "Oslo",
        "Ottawa",
        "Ouagadougou",
        "Ouahigouya",
        "Ouarzazate",
        "Oulu",
        "Palembang",
        "Palermo",
        "Palm Springs",
        "Palmerston North",
        "Panama City",
        "Parakou",
        "Paris",
        "Perth",
        "Petropavlovsk-Kamchatsky",
        "Philadelphia",
        "Phnom Penh",
        "Phoenix",
        "Pittsburgh",
        "Podgorica",
        "Pointe-Noire",
        "Pontianak",
        "Port Moresby",
        "Port Sudan",
        "Port Vila",
        "Port-Gentil",
        "Portland (OR)",
        "Porto",
        "Prague",
        "Praia",
        "Pretoria",
        "Pyongyang",
        "Rabat",
        "Rangpur",
        "Reggane",
        "Reykjavík",
        "Riga",
        "Riyadh",
        "Rome",
        "Roseau",
        "Rostov-on-Don",
        "Sacramento",
        "Saint Petersburg",
        "Saint-Pierre",
        "Salt Lake City",
        "San Antonio",
        "San Diego",
        "San Francisco",
        "San Jose",
        "San José",
        "San Juan",
        "San Salvador",
        "Sana'a",
        "Santo Domingo",
        "Sapporo",
        "Sarajevo",
        "Saskatoon",
        "Seattle",
        "Seoul",
        "Seville",
        "Shanghai",
        "Singapore",
        "Skopje",
        "Sochi",
        "Sofia",
        "Sokoto",
        "Split",
        "St. John's",
        "St. Louis",
        "Stockholm",
        "Surabaya",
        "Suva",
        "Suwałki",
        "Sydney",
        "Ségou",
        "Tabora",
        "Tabriz",
        "Taipei",
        "Tallinn",
        "Tamale",
        "Tamanrasset",
        "Tampa",
        "Tashkent",
        "Tauranga",
        "Tbilisi",
        "Tegucigalpa",
        "Tehran",
        "Tel Aviv",
        "Thessaloniki",
        "Thiès",
        "Tijuana",
        "Timbuktu",
        "Tirana",
        "Toamasina",
        "Tokyo",
        "Toliara",
        "Toluca",
        "Toronto",
        "Tripoli",
        "Tromsø",
        "Tucson",
        "Tunis",
        "Ulaanbaatar",
        "Upington",
        "Vaduz",
        "Valencia",
        "Valletta",
        "Vancouver",
        "Veracruz",
        "Vienna",
        "Vientiane",
        "Villahermosa",
        "Vilnius",
        "Virginia Beach",
        "Vladivostok",
        "Warsaw",
        "Washington, D.C.",
        "Wau",
        "Wellington",
        "Whitehorse",
        "Wichita",
        "Willemstad",
        "Winnipeg",
        "Wrocław",
        "Xi'an",
        "Yakutsk",
        "Yangon",
        "Yaoundé",
        "Yellowknife",
        "Yerevan",
        "Yinchuan",
        "Zagreb",
        "Zanzibar City",
        "Zürich",
        "Ürümqi",
        "İzmir",
    ];
}

fn init_lookup_structure() -> (PtrHash<&'static str>, Vec<AggregatedData>) {
    let mphf = <PtrHash<_>>::new(&ALL_STATIONS, PtrHashParams::default());
    // Map-like vector. Indices map to elements.
    let mut map = vec![AggregatedData::default(); ALL_STATIONS.len()];

    // Init all entries with data that is already known.
    for station in ALL_STATIONS {
        let index = mphf.index_minimal(&station);
        map[index].init(station);
    }

    (mphf, map)
}

/// Processes all data according to the 1brc challenge by using a
/// single-threaded implementation.
pub fn process_single_threaded(path: impl AsRef<Path> + Clone, print: bool) {
    let (_mmap, bytes) = unsafe { open_file(path) };

    let (hasher, lookup_structure) = init_lookup_structure();

    let stats = process_file_chunk(bytes, &hasher, lookup_structure);

    finalize([stats].into_iter(), &hasher, print);
}

/// Processes all data according to the 1brc challenge by using a
/// multi-threaded implementation. This spawns `n-1` worker threads. The main
/// thread also performs one workload and finally collects and combines all
/// results.
pub fn process_multi_threaded(path: impl AsRef<Path> + Clone, print: bool) {
    let (_mmap, bytes) = unsafe { open_file(path) };

    let (hasher, lookup_structure) = init_lookup_structure();

    let cpus = cpu_count(bytes.len());

    let mut thread_handles = Vec::with_capacity(cpus);

    let mut iter = ChunkIter::new(bytes, cpus);
    let main_thread_chunk = iter.next().unwrap();

    let hasher_ref = &hasher;

    for chunk in iter {
        // Spawning the threads is negligible cheap.
        // TODO it surprises me that rustc won't force me to transmute `chunk`
        //  to a &static lifetime.

        let hasher = unsafe { core::mem::transmute::<_, &'static _>(hasher_ref) };

        let lookup_structure = lookup_structure.clone();

        let handle = thread::spawn(move || process_file_chunk(chunk, &hasher, lookup_structure));
        thread_handles.push(handle);
    }

    let stats = process_file_chunk(main_thread_chunk, hasher_ref, lookup_structure);

    debug_assert_eq!(
        thread_handles.len(),
        cpus - 1,
        "must have 1-n worker threads"
    );

    let thread_results_iter = thread_handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .chain(core::iter::once(stats));

    finalize(thread_results_iter, &hasher, print);
}

/// Opens the file by mapping it via mmap into the address space of the program.
///
/// # Safety
/// The returned buffer is only valid as long as the returned `Mmap` lives.
unsafe fn open_file<'a>(path: impl AsRef<Path>) -> (Mmap, &'a [u8]) {
    let file = File::open(path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    // Only valid as long as `mmap` lives.
    let file_bytes: &[u8] = unsafe { slice::from_raw_parts(mmap.as_ptr(), mmap.len()) };

    (mmap, file_bytes)
}

/// Processes a chunk of the file. A chunk begins with the first byte of a line
/// and ends with a newline (`\n`).
///
/// The contained loop is the highly optimized hot path of the data processing.
/// There are no allocations, no unnecessary buffers, no unnecessary copies, no
/// unnecessary comparisons. I optimized the shit out of this :D
///
/// The returned data structure is not sorted.
fn process_file_chunk(
    bytes: &[u8],
    hasher: &PtrHash<&str>,
    mut lookup_structure: Vec<AggregatedData>,
) -> Vec<AggregatedData> {
    assert!(!bytes.is_empty());
    let &last_byte = bytes.last().unwrap();
    assert_eq!(last_byte, b'\n');

    // In each iteration, I read a line in two dedicated steps:
    // 1.) read city name
    // 2.) read value
    let mut consumed_bytes_count = 0;
    while consumed_bytes_count < bytes.len() {
        // Remaining bytes for this loop iteration. Each iteration processes
        // the bytes until the final newline.
        //
        // The following indices and ranges are relative within the bytes
        // representing the current line.
        let remaining_bytes = &bytes[consumed_bytes_count..];

        // Look for ";", and skip irrelevant bytes beforehand.
        let search_offset = MIN_STATION_LEN;
        let delimiter = memchr::memchr(b';', &remaining_bytes[search_offset..])
            .map(|pos| pos + search_offset)
            .unwrap();
        // Look for "\n", and skip irrelevant bytes beforehand.
        let search_offset = delimiter + 1 + MIN_MEASUREMENT_LEN;
        let newline = memchr::memchr(b'\n', &remaining_bytes[search_offset..])
            .map(|pos| pos + search_offset)
            .unwrap();

        let station = unsafe { from_utf8_unchecked(&remaining_bytes[0..delimiter]) };
        let measurement = unsafe { from_utf8_unchecked(&remaining_bytes[delimiter + 1..newline]) };

        let measurement = fast_f32_parse_encoded(measurement);

        // Ensure the next iteration works on the next line.
        consumed_bytes_count += newline + 1;

        let index = hasher.index_minimal(&station);

        unsafe {
            lookup_structure
                .get_unchecked_mut(index)
                .add_datapoint(measurement);
        }
    }
    lookup_structure
}

fn cpu_count(size: usize) -> usize {
    if size < 10000 {
        1
    } else {
        available_parallelism().unwrap().into()
    }
}

/// Optimized fast decimal number parsing that encodes a float in an integer,
/// which is multiplied by 10.
///
/// This benefits from the fact that we know that all input data has exactly 1
/// decimal place.
///
/// - `15.5` -> `155`
/// - `-7.1` -> `-71`
///
/// The range of possible values is within `-99.9..=99.9`.
///
/// To get back to the actual floating point value, one has to convert the value
/// to float and divide it by 10.
fn fast_f32_parse_encoded(input: &str) -> i16 {
    let mut bytes = input.as_bytes();

    let negative = bytes[0] == b'-';

    if negative {
        // Only parse digits.
        bytes = &bytes[1..];
    }

    let mut val = 0;
    for &byte in bytes {
        if byte == b'.' {
            continue;
        }
        let digit = (byte - b'0') as i16;
        val = val * 10 + digit;
    }

    if negative {
        -val
    } else {
        val
    }
}

/// Aggregates the results and, optionally, prints them.
fn finalize<'a>(
    stats: impl Iterator<Item = Vec<AggregatedData>>,
    hasher: &PtrHash<&str>,
    print: bool,
) {
    let mut combined_results = vec![AggregatedData::default(); STATIONS_IN_DATASET];

    // This reduce step is surprisingly negligible cheap.
    for vector in stats {
        for elem in vector {
            let index = hasher.index_minimal(&elem.name());

            if combined_results[index].name().is_empty() {
                combined_results[index].init(elem.name());
            }

            combined_results[index].merge(&elem);
        }
    }

    // Sort everything into a vector. The costs of this are negligible cheap.
    combined_results
        .sort_unstable_by(|data_a, data_b| data_a.name().partial_cmp(data_b.name()).unwrap());

    if print {
        print_results(combined_results.into_iter())
    } else {
        // black-box: prevent the compiler from optimizing any calculations away
        let _x = black_box(combined_results);
    }
}

/// Prints the results. The costs of this function are negligible cheap.
fn print_results<'a>(stats: impl ExactSizeIterator<Item = AggregatedData>) {
    print!("{{");
    let n = stats.len();
    stats
        .enumerate()
        .map(|(index, val)| (index == n - 1, val))
        .for_each(|(is_last, val)| {
            print!(
                "{}={:.1}/{:.1}/{:.1}",
                val.name(),
                val.min(),
                val.avg(),
                val.max()
            );
            if !is_last {
                print!(", ");
            }
        });
    println!("}}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_file_chunk() {
        let input = "Berlin;10.0\nHamburg;-12.7\nNew York;21.5\nBerlin;-15.7\n";
        let actual = process_file_chunk(input.as_bytes());
        let stats = actual.into_iter().collect::<Vec<_>>();

        // Order here is not relevant. I stick to the order from the HashMap
        // implementation.
        let hamburg = &stats[0];
        let berlin = &stats[1];
        let new_york = &stats[2];

        assert_eq!(hamburg.0, "Hamburg");
        assert_eq!(berlin.0, "Berlin");
        assert_eq!(new_york.0, "New York");

        let hamburg = &hamburg.1;
        let berlin = &berlin.1;
        let new_york = &new_york.1;

        assert_eq!(hamburg, &AggregatedData::new(-127, -127, -127, 1));
        assert_eq!(berlin, &AggregatedData::new(-157, 100, -57, 2));
        assert_eq!(new_york, &AggregatedData::new(215, 215, 215, 1));

        assert_eq!(hamburg.avg(), -12.7);
        assert_eq!(berlin.avg(), -2.85);
        assert_eq!(new_york.avg(), 21.5);
    }

    #[test]
    fn test_fast_f32_parse() {
        assert_eq!(fast_f32_parse_encoded("0.0"), 00);
        assert_eq!(fast_f32_parse_encoded("5.0"), 50);
        assert_eq!(fast_f32_parse_encoded("5.7"), 57);
        assert_eq!(fast_f32_parse_encoded("-5.7"), -57);
        assert_eq!(fast_f32_parse_encoded("-99.9"), -999);
    }
}
