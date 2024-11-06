import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

extension Error {

  /// Whether this error represents a "zone not found" or a "user deleted zone" error
  var isCloudKitZoneDeleted: Bool {
    guard let effectiveError = self as? CKError else { return false }

    return [.zoneNotFound, .userDeletedZone].contains(effectiveError.code)
  }

  /// Uses the `resolver` closure to resolve a conflict, returning the conflict-free record
  ///
  /// - Parameter resolver: A closure that will receive the client record as the first param and the server record as the second param.
  /// This closure is responsible for handling the conflict and returning the conflict-free record.
  /// - Returns: The conflict-free record returned by `resolver`
  func resolveConflict<Persistable: CloudKitCodable>(
    _ logger: ((String, OSLogType) -> Void)? = nil,
    with resolver: (Persistable, Persistable) -> Persistable?
  ) -> CKRecord? {
    guard let effectiveError = self as? CKError else {
      logger?(
        "resolveConflict called on an error that was not a CKError. The error was \(String(describing: self))",
        .fault)
      return nil
    }

    guard effectiveError.code == .serverRecordChanged else {
      logger?(
        "resolveConflict called on a CKError that was not a serverRecordChanged error. The error was \(String(describing: effectiveError))",
        .fault)
      return nil
    }

    guard let clientRecord = effectiveError.clientRecord else {
      logger?(
        "Failed to obtain client record from serverRecordChanged error. The error was \(String(describing: effectiveError))",
        .fault)
      return nil
    }
    
    // Special handling for "record already exists" case
    if effectiveError.localizedDescription.contains("record to insert already exists"),
       let ancestorRecord = effectiveError.ancestorRecord {
      // Use the ancestor record as the server record since it represents
      // the last known server state
      logger?("Using ancestor record for already-exists conflict", .error)
      
      // Debug logging
      logger?("Ancestor Record Type: \(ancestorRecord.recordType)", .debug)
      logger?("Ancestor Record Keys: \(ancestorRecord.allKeys())", .debug)
      logger?("Client Record Keys: \(clientRecord.allKeys())", .debug)
      
      // If ancestor record is empty, use client record as base
      if ancestorRecord.allKeys().isEmpty {
        logger?("Ancestor record is empty, using client record as base", .info)
        
        // Create a new record with the same ID but using client data
        let newRecord = CKRecord(recordType: clientRecord.recordType,
                                 recordID: clientRecord.recordID)
        
        // Copy all fields from client record
        clientRecord.allKeys().forEach { key in
          newRecord[key] = clientRecord[key]
        }
        
        return newRecord
      }
      
      guard
        let clientPersistable = try? CKRecordDecoder().decode(
          Persistable.self, from: clientRecord)
      else { return nil }
      
      logger?("Client decode succeeded", .debug)
      
      guard let ancestorPersistable = try? CKRecordDecoder().decode(
          Persistable.self, from: ancestorRecord)
      else { return nil }
      
      logger?("Ancestor decode succeeded", .debug)
      
      guard let resolvedPersistable = resolver(clientPersistable, ancestorPersistable)
      else { return nil }
      
      guard let resolvedRecord = try? CKRecordEncoder(zoneID: ancestorRecord.recordID.zoneID).encode(
          resolvedPersistable)
      else { return nil }
      
      if resolvedPersistable == clientPersistable {
        logger?("Resolved record: local", .info)
      }
      else {
        logger?("Resolved record: server", .info)
      }
      
      logger?("Resolved record succeeded", .debug)
      
      // Get all possible keys through reflection
      let mirror = Mirror(reflecting: resolvedPersistable)
      let allPossibleKeys = mirror.children.compactMap { $0.label }
      
      // Update all keys, including nil values
      allPossibleKeys.forEach { key in
        // This will properly set nil values as well
        ancestorRecord[key] = resolvedRecord[key]
      }
      
      return ancestorRecord
    }

    guard let serverRecord = effectiveError.serverRecord else {
      logger?(
        "Failed to obtain server record from serverRecordChanged error. The error was \(String(describing: effectiveError))",
        .fault)
      return nil
    }

    logger?(
      "CloudKit conflict with record of type \(serverRecord.recordType). Running conflict resolver",
      .error)

    // Always return the server record so we don't end up in a conflict loop (the server record has the change tag we want to use)
    // https://developer.apple.com/documentation/cloudkit/ckerror/2325208-serverrecordchanged
    
    guard
      let clientPersistable = try? CKRecordDecoder().decode(
        Persistable.self, from: clientRecord)
    else { return nil }
    
    guard
      let serverPersistable = try? CKRecordDecoder().decode(
        Persistable.self, from: serverRecord)
    else { return nil }
    
    guard
      let resolvedPersistable = resolver(clientPersistable, serverPersistable)
    else { return nil }
    
    guard
      let resolvedRecord = try? CKRecordEncoder(zoneID: serverRecord.recordID.zoneID).encode(
        resolvedPersistable)
    else { return nil }
    
    if resolvedPersistable == clientPersistable {
      logger?("Resolved record: local", .info)
    }
    else {
      logger?("Resolved record: server", .info)
    }
    
    // Get all possible keys through reflection
    let mirror = Mirror(reflecting: resolvedPersistable)
    let allPossibleKeys = mirror.children.compactMap { $0.label }
    
    // Update all keys, including nil values
    allPossibleKeys.forEach { key in
      // This will properly set nil values as well
      serverRecord[key] = resolvedRecord[key]
    }
    
    return serverRecord
  }

  /// Retries a CloudKit operation if the error suggests it
  /// 
  /// - Parameters:
  ///   - logger: The logger to use for logging information about the error handling, uses the default one if not set
  ///   - block: The block that will execute the operation later if it can be retried
  ///   - queue: queue to run the block on
  ///
  /// - Returns: Whether or not it was possible to retry the operation
  @discardableResult func retryCloudKitOperationIfPossible(_ logger: ((String, OSLogType) -> Void)? = nil, 
                                                           queue: DispatchQueue,
                                                           with block: @escaping () -> Void) -> Bool {
    guard let effectiveError = self as? CKError else { return false }
    
    let retryDelay: Double
    
    // Handle token expiry, both direct and within partial failures
    if effectiveError.code == .changeTokenExpired {
      retryDelay = 0
    }
    else if effectiveError.code == .partialFailure, let partialErrors = effectiveError.userInfo[CKPartialErrorsByItemIDKey] as? [AnyHashable: Error], partialErrors.values.contains(where: { ($0 as? CKError)?.code == .changeTokenExpired }) == true {
      retryDelay = 0
    }
    else if let suggestedRetryDelay = effectiveError.retryAfterSeconds {
      retryDelay = suggestedRetryDelay
    } 
    else if effectiveError.code == CKError.Code.limitExceeded {
      retryDelay = 0
    } 
    else {
      logger?("Error is not recoverable", .error)
      return false
    }

    logger?("Error is recoverable. Will retry after \(retryDelay) seconds", .error)

    queue.asyncAfter(deadline: .now() + retryDelay) {
      block()
    }

    return true
  }

}
