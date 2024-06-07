import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

final class UploadRecordContext<Persistable: CloudKitCodable>: RecordModifyingContextProvider {

  let name = "upload"

  private let defaults: UserDefaults
  private let zoneID: CKRecordZone.ID
  private let logHandler: (String, OSLogType) -> Void

  init(
    defaults: UserDefaults, zoneID: CKRecordZone.ID,
    logHandler: @escaping (String, OSLogType) -> Void
  ) {
    self.defaults = defaults
    self.zoneID = zoneID
    self.logHandler = logHandler
  }

  // MARK: - RecordModifying
  
  func encodedRecord<T: CloudKitCodable>(_ obj: T) throws -> CKRecord {
    try CKRecordEncoder(zoneID: zoneID).encode(obj)
  }
  
  func modelChangeForUpdatedRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges {
    let models: Set<T> = Set(
      recordsSaved.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(T.self, from: record)
        } catch {
          logHandler("Error decoding item from record: \(String(describing: error))", .error)
          return nil
        }
      })

    return .init(updates: .updatesPushed(models))
  }

  func failedToUpdateRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges {
    let models: Set<T> = Set(
      recordsSaved.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(T.self, from: record)
        } catch {
          logHandler("Error decoding item from record: \(String(describing: error))", .error)
          return nil
        }
      })
    
    return .init(unknownUpdates: .unknownItemsPushed(models))
  }
}
