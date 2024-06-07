import CloudKit
import CloudKitCodable
import Foundation

protocol RecordModifyingContextProvider {
  var name: String { get }
  
  func encodedRecord<T: CloudKitCodable>(_ obj: T) throws -> CKRecord
  func modelChangeForUpdatedRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges
  func failedToUpdateRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges
}
