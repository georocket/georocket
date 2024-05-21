use super::index_map::IdIndexMap;
use crate::store::channels::StoreChannels;
use crate::types::{Index, IndexElement, RawChunk};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use uuid::{fmt::Simple, Uuid};

#[derive(Debug, Deserialize, Serialize)]
struct InternalIndex {
    uuid: Uuid,
    index_elements: Vec<IndexElement>,
}

/// Helper function to generate a path and file name for a given `Uuid`.
fn make_path(uuid: Uuid) -> (PathBuf, PathBuf) {
    let uuid_string: UuidString = uuid.into();
    let strings = uuid_string
        .0
        .chunks_exact(8)
        .map(|chunk| std::str::from_utf8(chunk).unwrap());
    let mut path: PathBuf = PathBuf::new();
    let file_name = strings.clone().last().unwrap().into();
    for component in strings.take(3) {
        path.push(component)
    }
    (path, file_name)
}

/// A convenience wrapper around an array containing the ascii representation of a `Uuid`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct UuidString([u8; Simple::LENGTH]);

impl AsRef<str> for UuidString {
    fn as_ref(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }
}
impl From<Uuid> for UuidString {
    fn from(uuid: Uuid) -> Self {
        let mut contents = [0u8; Simple::LENGTH];
        uuid.simple().encode_lower(&mut contents);
        Self(contents)
    }
}

impl From<UuidString> for Uuid {
    fn from(uuid_string: UuidString) -> Self {
        Uuid::parse_str(uuid_string.as_ref()).unwrap()
    }
}

/// The `FileStore` receives `RawChunk`s and `Index`es and writes them to disk.
/// The 'Index'es are written to a single index file, while the `RawChunk`s are
/// written to files in a directory structure based on randomly generated `Uuid`s, which
/// are stored in the index file. The `FileStore` will append to an existing index file or
/// create a new one if none exists.
pub struct FileStore {
    directory: PathBuf,
    store_channels: StoreChannels,
    index_map: IdIndexMap,
    index_file: BufWriter<File>,
}

impl FileStore {
    /// Creates a new `FileStore` object and creates an index file in the specified directory.
    pub async fn new(
        directory: impl Into<PathBuf>,
        store_channels: StoreChannels,
    ) -> anyhow::Result<Self> {
        let directory = directory.into();
        let index_file = Self::create_index_file(&directory).await?;
        Ok(Self {
            directory,
            store_channels,
            index_file,
            index_map: IdIndexMap::new(),
        })
    }

    /// Create the index file in or open it for appending.
    async fn create_index_file(directory: &Path) -> anyhow::Result<BufWriter<File>> {
        let index_path = Path::join(directory, "index.geo");
        let index_file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_path)
            .await?;
        let index_file = BufWriter::new(index_file);
        Ok(index_file)
    }

    /// Consumes the `FileStore`. The task will run until both the `RawChunk` and `Index` channels
    /// are closed. If successful, the number of `Index`es written to the index file is returned.
    /// If the number of `Index`es does not match the number of `RawChunk`s, an error documenting
    /// the mismatch is returned.
    ///
    /// # Errors
    /// If an error occurs while writing to the index file, the function aborts and returns the error.
    pub async fn run(&mut self) -> anyhow::Result<usize> {
        let mut num_indexes = 0;
        let mut num_chunks = 0;
        loop {
            tokio::select! {
                Some(raw) = self.store_channels.raw_rec.recv() => {
                        self.handle_chunk(raw).await?;
                        num_chunks += 1;
                }
                Some(index) = self.store_channels.index_rec.recv() => {
                    self.handle_index(index).await?;
                    num_indexes += 1;
                }
                else => {
                    break;
                }
            }
        }
        self.index_file.flush().await?;
        if num_chunks == num_indexes {
            Ok(num_indexes)
        } else {
            Err(anyhow::anyhow!(
                "Number of chunks ({}) written does not match number of indexes ({}) written",
                num_chunks,
                num_indexes
            ))
        }
    }

    async fn handle_chunk(&mut self, chunk: RawChunk) -> anyhow::Result<()> {
        Self::handle_chunk_helper(&mut self.index_map, self.directory.clone(), chunk).await
    }

    /// Writes chunks to disk. Generates a `Uuid` for the chunk if one does not already exist for the chunk/index pair.
    async fn handle_chunk_helper(
        index_map: &mut IdIndexMap,
        mut directory: PathBuf,
        chunk: RawChunk,
    ) -> anyhow::Result<()> {
        let RawChunk { id, raw, .. } = chunk;
        let uuid = index_map.get_or_create_index(id);
        let (file_dir, file_name) = make_path(uuid);
        directory.push(file_dir);
        tokio::fs::create_dir_all(&directory).await?;
        directory.push(file_name);
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(directory)
            .await?;
        file.write_all(&raw).await?;
        Ok(())
    }

    async fn handle_index(&mut self, index: Index) -> anyhow::Result<()> {
        Self::handle_index_helper(&mut self.index_file, &mut self.index_map, index).await
    }

    /// Writes indexes to the index file. Generates a `Uuid` for the index if one does not already exist
    /// for the chunk/index pair.
    async fn handle_index_helper<File: AsyncWrite + Unpin>(
        file: &mut File,
        index_map: &mut IdIndexMap,
        index: Index,
    ) -> anyhow::Result<()> {
        let Index { id, index_elements } = index;
        let uuid: Uuid = index_map.get_or_create_index(id);
        let index = InternalIndex {
            uuid,
            index_elements,
        };
        let json_index = serde_json::to_string(&index)?;
        file.write_all(json_index.as_bytes()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use indexing::bounding_box::{BoundingBoxBuilder, NoValidation};
    use std::collections::HashMap;
    use std::fs::DirBuilder;
    use std::io::Cursor;

    #[test]
    fn test_uuid_to_path() {
        use std::path::PathBuf;
        use uuid::Uuid;
        let uuid = Uuid::parse_str("000102030405060708090a0b0c0d0e0f").unwrap();
        let (path, file_name) = make_path(uuid);
        let control_path = PathBuf::from("00010203/04050607/08090a0b");
        let control_file_name = PathBuf::from("0c0d0e0f");
        assert_eq!(path, control_path);
        assert_eq!(file_name, control_file_name);
    }

    #[test]
    /// Test the conversion of a UUID to a UuidString and back
    fn test_uuid_string() {
        test_uuid_string_helper("000102030405060708090a0b0c0d0e0f");
        test_uuid_string_helper("00000000000000000000000000000000");
        test_uuid_string_helper("ffffffffffffffffffffffffffffffff");
    }

    fn test_uuid_string_helper(uuid_str: &str) {
        let uuid = Uuid::parse_str(uuid_str).unwrap();
        let uuid_string: UuidString = uuid.into();
        assert_eq!(uuid_string.as_ref(), uuid_str);
        let uuid_from_string: Uuid = uuid_string.into();
        assert_eq!(uuid, uuid_from_string);
    }

    #[test]
    /// Test serialization and deserialization of a UuidString
    fn test_uuid_string_ser_deserialization() {
        test_uuid_string_ser_deserialization_helper("000102030405060708090a0b0c0d0e0f");
        test_uuid_string_ser_deserialization_helper("00000000000000000000000000000000");
        test_uuid_string_ser_deserialization_helper("ffffffffffffffffffffffffffffffff");
    }

    fn test_uuid_string_ser_deserialization_helper(uuid: &str) {
        let uuid = Uuid::parse_str(uuid).unwrap();
        let uuid_string: UuidString = uuid.into();
        let json = serde_json::to_string(&uuid_string).unwrap();
        let uuid_string_deserialized: UuidString = serde_json::from_str(&json).unwrap();
        assert_eq!(uuid_string, uuid_string_deserialized);
    }

    async fn test_write_index_helper(indexes: &[Index]) {
        let mut index_map = IdIndexMap::new();
        let mut index_file = Cursor::new(Vec::new());
        for index in indexes.iter().cloned() {
            FileStore::handle_index_helper(&mut index_file, &mut index_map, index)
                .await
                .unwrap();
        }
        index_file.set_position(0);
        let index_stream =
            serde_json::Deserializer::from_reader(index_file).into_iter::<InternalIndex>();
        for (index, internal_index) in indexes.iter().zip(index_stream.map(|i| i.unwrap())) {
            // check that the correct uuid was written for the index
            assert_eq!(internal_index.uuid, index_map.0[&index.id]);
            // check that the index elements are the same between the original index and the deserialized index
            assert_eq!(internal_index.index_elements, index.index_elements);
        }
    }

    #[tokio::test]
    async fn test_write_index() {
        let empty_index = [];
        let simple_indexes = [Index {
            id: 0,
            index_elements: vec![],
        }];
        let coordinates = (1..).map(|i| {
            let i = i as f64;
            [(-i, -i), (i, i)]
        });
        let complex_indexes = coordinates
            .take(5)
            .enumerate()
            .map(|(id, coords)| {
                let mut bounding_box = BoundingBoxBuilder::new(NoValidation);
                for (x, y) in coords {
                    bounding_box = bounding_box.add_point(x, y);
                }
                let bounding_box = bounding_box.build().unwrap().unwrap();
                Index {
                    id,
                    index_elements: vec![IndexElement::BoundingBoxIndex(bounding_box)],
                }
            })
            .collect::<Vec<_>>();
        test_write_index_helper(&empty_index).await;
        test_write_index_helper(&simple_indexes).await;
        test_write_index_helper(&complex_indexes).await;
    }

    async fn test_write_chunks_helper(chunks: &[RawChunk], mut id_index_map: IdIndexMap) {
        // create a map from id to chunk, so we can look up the chunk for a given id in constant time later
        let id_to_chunk = chunks
            .iter()
            .cloned()
            .map(|chunk| (chunk.id, chunk.raw))
            .collect::<HashMap<_, _>>();
        // create our base directory for writing the chunks
        let directory: PathBuf = {
            let mut directory = std::env::temp_dir();
            directory.push(Uuid::new_v4().to_string());
            directory
        };
        DirBuilder::new().create(&directory).unwrap();
        for chunk in chunks.iter().cloned() {
            FileStore::handle_chunk_helper(&mut id_index_map, directory.clone(), chunk)
                .await
                .unwrap();
        }
        for (id, uuid) in id_index_map.0 {
            let path = {
                let mut path = directory.clone();
                let (file_path, file_name) = make_path(uuid);
                path.push(file_path);
                path.push(file_name);
                path
            };
            let file_contents = std::fs::read(path).unwrap();
            assert_eq!(file_contents, id_to_chunk[&id]);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn test_write_chunks() {
        let empty_chunks = [];
        let simple_chunks = [RawChunk {
            id: 0,
            raw: vec![],
            meta: None,
        }];
        let complex_chunks = [
            RawChunk {
                id: 0,
                raw: vec![0, 1, 2, 3, 4, 5, 6, 7],
                meta: None,
            },
            RawChunk {
                id: 1,
                raw: vec![8, 9, 10, 11, 12, 13, 14, 15],
                meta: None,
            },
            RawChunk {
                id: 2,
                raw: vec![16, 17, 18, 19, 20, 21, 22, 23],
                meta: None,
            },
            RawChunk {
                id: 3,
                raw: vec![24, 25, 26, 27, 28, 29, 30, 31],
                meta: None,
            },
            RawChunk {
                id: 4,
                raw: vec![32, 33, 34, 35, 36, 37, 38, 39],
                meta: None,
            },
        ];
        let many_chunks = (0..10_000)
            .map(|i| RawChunk {
                id: i,
                raw: vec![i as u8; 8],
                meta: None,
            })
            .collect::<Vec<_>>();

        // test without existing ID-Uuid pairs:
        test_write_chunks_helper(&empty_chunks, IdIndexMap::new()).await;
        test_write_chunks_helper(&simple_chunks, IdIndexMap::new()).await;
        test_write_chunks_helper(&complex_chunks, IdIndexMap::new()).await;
        test_write_chunks_helper(&many_chunks, IdIndexMap::new()).await;
        // test with existing ID-Uuid pairs:
        let simple_id_index_map: IdIndexMap = (0..1)
            .map(|i| (i, Uuid::new_v4()))
            .collect::<HashMap<_, _>>()
            .into();
        let complex_id_index_map: IdIndexMap = (0..complex_chunks.len())
            .map(|i| (i, Uuid::new_v4()))
            .collect::<HashMap<_, _>>()
            .into();
        let many_id_index_map: IdIndexMap = (0..many_chunks.len())
            .map(|i| (i, Uuid::new_v4()))
            .collect::<HashMap<_, _>>()
            .into();
        test_write_chunks_helper(&simple_chunks, simple_id_index_map).await;
        test_write_chunks_helper(&complex_chunks, complex_id_index_map).await;
        test_write_chunks_helper(&many_chunks, many_id_index_map).await;
    }
}
