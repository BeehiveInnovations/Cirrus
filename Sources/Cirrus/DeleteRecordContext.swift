import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

final class DeleteRecordContext<Persistable: CloudKitCodable>: RecordModifyingContextProvider {

  let name = "delete"

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

  func modelChangeForUpdatedRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], 
                                                        recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges {
    let recordIdentifiersDeletedSet = Set(recordIDsDeleted.map(\.recordName))

    return .init(deletes: .deletesPushed(recordIdentifiersDeletedSet))
  }

  func failedToUpdateRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges {
    let recordIdentifiersDeletedSet = Set(recordIDsDeleted.map(\.recordName))
    
    return .init(unknownDeletes: .unknownItemsDeleted(recordIdentifiersDeletedSet))
  }
}
